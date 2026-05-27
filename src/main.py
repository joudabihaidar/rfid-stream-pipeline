# src/main.py
import openpyxl
from pathlib import Path
from typing import Dict, List

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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXCEL_PATH   = PROJECT_ROOT / "data" / "raw_data.xlsx"


def print_events(events: List[Dict]):
    """Prints confirmed events to the terminal."""
    for e in events:
        tag   = f"[{e['event']:<16}]"
        t0    = str(e["t0"]) if e["t0"] is not None else "N/A"
        door  = e.get("door", "")
        zone  = (
            f"{e.get('from_zone','').split('__')[-1]}"
            f" → "
            f"{e.get('to_zone','').split('__')[-1]}"
            if e["event"] == "ZONE_TRANSITION" else ""
        )
        dwell = f"  dwell={e['dwell_time']}s" if e.get("dwell_time") is not None else ""
        note  = f"  {e['note']}"              if e.get("note")       else ""
        print(f"  {tag}  t0={t0:<4}  {door or zone}{dwell}{note}")


def run_all_sheets(file_path: str) -> Dict[str, List[Dict]]:
    """
    Full two-stage pipeline for every sheet in the workbook.

    Stage 1 — Ingestion:
        Raw RFID rows are streamed from Excel and written to the
        raw_reads table exactly as received. No transformation.

    Stage 2 — Processing:
        Raw rows are read back from the DB, run through the detection
        algorithm, and the resulting events and anomalies are stored.

    Returns a dict of {session_id: events} for all sessions.
    """
    wb = openpyxl.load_workbook(file_path, read_only=True)
    sheet_names = wb.sheetnames
    wb.close()

    all_results = {}

    for sheet_name in sheet_names:
        print(f"\n{'═'*60}")
        print(f"  Processing sheet: {sheet_name}")
        print(f"{'═'*60}")

        # ── Stage 1: ingest raw reads into DB ────────────────────
        print("  [1/5] Ingesting raw reads...")
        ingest_raw_reads(str(file_path), sheet_name)

        # ── Stage 2a: read back from DB ──────────────────────────
        print("  [2/5] Reading from database...")
        db_rows = get_raw_rows(sheet_name)

        if not db_rows:
            print("  No rows found — skipping.")
            continue

        # ── Stage 2b: run detection pipeline ─────────────────────
        print("  [3/5] Running detection pipeline...")
        events = run_pipeline_from_rows(db_rows)

        if not events:
            print("  No events detected.")
            continue

        # ── Stage 2c: detect anomalies and compute quality ───────────
        print("  [4/5] Detecting anomalies...")
        anomalies = detect_anomalies(events, db_rows)
        quality   = compute_data_quality(events, db_rows)

        # ── Stage 2d: store results ───────────────────────────────────
        print("  [5/5] Storing results...")
        insert_session(sheet_name, events, has_anomaly=len(anomalies) > 0)
        insert_events(sheet_name, events)
        insert_anomalies(sheet_name, anomalies)
        update_session_quality(sheet_name, quality)

        all_results[sheet_name] = events
        print()
        print_events(events)

    return all_results


if __name__ == "__main__":
    # Create DB schema once — safe to call every run
    create_schema()

    results = run_all_sheets(EXCEL_PATH)

    print(f"\n{'═'*60}")
    print(f"  SUMMARY")
    print(f"{'═'*60}")
    for session, events in results.items():
        entries = sum(1 for e in events if e["event"] == "ENTRY")
        exits   = sum(1 for e in events if e["event"] == "EXIT")
        anomaly = sum(1 for e in events if e["event"] == "SESSION_ENDED_INSIDE")
        print(f"  {session:<35}  entries={entries}  exits={exits}  flags={anomaly}")

    print(f"\n  Database written to: {PROJECT_ROOT / 'data' / 'rfid_events.db'}")