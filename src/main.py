# src/main.py
import logging
import sys
import openpyxl
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Allow imports from repo root (e.g. analytics/) when running `python src/main.py`
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import run_pipeline_from_rows
from database import (
    create_schema,
    ingest_raw_reads,
    get_raw_rows,
    insert_session,
    insert_events,
    insert_anomalies,
    update_session_quality,
)
from analytics.anomaly import detect_anomalies
from analytics.quality import compute_data_quality

EXCEL_PATH   = PROJECT_ROOT / "data" / "raw_data.xlsx"
LOG_PATH     = PROJECT_ROOT / "data" / "pipeline.log"

# ═══════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════

def setup_logging():
    """
    Configures logging to write to both the terminal and a log file.

    Terminal: INFO and above, clean format for readability.
    Log file: DEBUG and above, timestamped for diagnostics.
    The log file lives in data/pipeline.log alongside the database.
    """
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    log_format = "%(asctime)s  %(levelname)-8s  %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        datefmt=date_format,
        handlers=[
            # Terminal — INFO and above only (keep it readable)
            logging.StreamHandler(),
            # File — everything including DEBUG
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
        ],
    )
    # Silence openpyxl's verbose DEBUG output
    logging.getLogger("openpyxl").setLevel(logging.WARNING)

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# DISPLAY
# ═══════════════════════════════════════════════════════════════

def print_events(events: List[Dict]):
    """Prints confirmed events to the terminal."""
    for e in events:
        tag   = f"[{e['event']:<16}]"
        t0    = str(e["t0"]) if e["t0"] is not None else "N/A"
        door  = e.get("door", "")
        zone  = (
            f"{e.get('from_zone', '').split('__')[-1]}"
            f" → "
            f"{e.get('to_zone', '').split('__')[-1]}"
            if e["event"] == "ZONE_TRANSITION" else ""
        )
        dwell = f"  dwell={e['dwell_time']}s" if e.get("dwell_time") is not None else ""
        note  = f"  {e['note']}"              if e.get("note")        else ""
        print(f"  {tag}  t0={t0:<4}  {door or zone}{dwell}{note}")


# ═══════════════════════════════════════════════════════════════
# PIPELINE
# ═══════════════════════════════════════════════════════════════

def process_sheet(file_path: str, sheet_name: str) -> List[Dict]:
    """
    Runs the full two-stage pipeline for a single sheet.

    Stage 1 — Ingestion:
        Raw RFID rows are streamed from Excel and written to the
        raw_reads table exactly as received. No transformation.

    Stage 2 — Processing:
        Raw rows are read back from the DB, run through the detection
        algorithm, and the resulting events and anomalies are stored.

    Returns the list of confirmed events, or an empty list on failure.
    Raises no exceptions — all errors are logged and the sheet is skipped.
    """

    # ── Stage 1: ingest raw reads ─────────────────────────────────
    log.info(f"[{sheet_name}] [1/5] Ingesting raw reads...")
    try:
        ingest_raw_reads(file_path, sheet_name)
    except Exception as e:
        log.error(f"[{sheet_name}] Ingestion failed: {e}")
        return []

    # ── Stage 2a: read back from DB ───────────────────────────────
    log.info(f"[{sheet_name}] [2/5] Reading from database...")
    try:
        db_rows = get_raw_rows(sheet_name)
    except Exception as e:
        log.error(f"[{sheet_name}] DB read failed: {e}")
        return []

    if not db_rows:
        log.warning(f"[{sheet_name}] No rows found after ingestion — skipping.")
        return []

    log.debug(f"[{sheet_name}] {len(db_rows)} raw rows loaded from DB.")

    # ── Stage 2b: run detection pipeline ─────────────────────────
    log.info(f"[{sheet_name}] [3/5] Running detection pipeline...")
    try:
        events = run_pipeline_from_rows(db_rows)
    except Exception as e:
        log.error(f"[{sheet_name}] Pipeline failed: {e}")
        return []

    if not events:
        log.warning(f"[{sheet_name}] No events detected.")
        return []

    log.debug(f"[{sheet_name}] {len(events)} events detected.")

    # ── Stage 2c: detect anomalies + compute quality ──────────────
    log.info(f"[{sheet_name}] [4/5] Detecting anomalies...")
    try:
        anomalies = detect_anomalies(events, db_rows)
        quality   = compute_data_quality(events, db_rows)
    except Exception as e:
        log.error(f"[{sheet_name}] Anomaly detection failed: {e}")
        anomalies = []
        quality   = {}

    log.debug(
        f"[{sheet_name}] {len(anomalies)} anomalies detected. "
        f"Ghost ratio: {quality.get('ghost_read_ratio')}  "
        f"Entry signal: {quality.get('entry_rssi_strength')}"
    )

    # ── Stage 2d: store results ───────────────────────────────────
    log.info(f"[{sheet_name}] [5/5] Storing results...")
    try:
        insert_session(sheet_name, events, has_anomaly=len(anomalies) > 0)
        insert_events(sheet_name, events)
        insert_anomalies(sheet_name, anomalies)
        update_session_quality(sheet_name, quality)
    except Exception as e:
        log.error(f"[{sheet_name}] DB write failed: {e}")
        return []

    log.info(f"[{sheet_name}] Done — {len(events)} events, {len(anomalies)} anomalies.")
    return events


def run_all_sheets(file_path: str) -> Dict[str, List[Dict]]:
    """
    Runs the full pipeline for every sheet in the workbook.
    Sheets that fail are logged and skipped — one bad sheet
    does not stop the rest from processing.

    Returns a dict of {session_id: events} for all successful sheets.
    """
    log.info(f"Opening workbook: {file_path}")
    try:
        wb = openpyxl.load_workbook(file_path, read_only=True)
        sheet_names = wb.sheetnames
        wb.close()
    except FileNotFoundError:
        log.error(f"Excel file not found: {file_path}")
        return {}
    except Exception as e:
        log.error(f"Failed to open workbook: {e}")
        return {}

    log.info(f"Found {len(sheet_names)} sheets: {sheet_names}")
    all_results = {}

    for sheet_name in sheet_names:
        # Use ASCII separators so Windows cp1252 consoles don't crash
        print("\n" + "=" * 60)
        print(f"  Processing sheet: {sheet_name}")
        print("=" * 60)

        events = process_sheet(str(file_path), sheet_name)

        if events:
            all_results[sheet_name] = events
            print()
            print_events(events)
        else:
            print(f"  ✗ Sheet skipped — see log for details.")

    return all_results


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    setup_logging()
    log.info("Pipeline started.")

    try:
        create_schema()
    except Exception as e:
        log.error(f"Failed to create DB schema: {e}")
        raise SystemExit(1)

    results = run_all_sheets(EXCEL_PATH)

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for session, events in results.items():
        entries = sum(1 for e in events if e["event"] == "ENTRY")
        exits   = sum(1 for e in events if e["event"] == "EXIT")
        flags   = sum(1 for e in events if e["event"] == "SESSION_ENDED_INSIDE")
        print(f"  {session:<35}  entries={entries}  exits={exits}  flags={flags}")

    print(f"\n  Database: {PROJECT_ROOT / 'data' / 'rfid_events.db'}")
    print(f"  Log file: {LOG_PATH}")
    log.info(f"Pipeline finished. {len(results)} sheets processed successfully.")