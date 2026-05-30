import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH      = PROJECT_ROOT / "data" / "rfid_events.db"


# ═══════════════════════════════════════════════════════════════
# CONNECTION
# ═══════════════════════════════════════════════════════════════

def get_connection() -> sqlite3.Connection:
    """
    Returns a connection to the SQLite database.
    row_factory=sqlite3.Row makes rows behave like dicts —
    access columns by name instead of index.
    Creates the data/ directory if it doesn't exist yet.

    WAL (Write-Ahead Logging) mode is enabled so multiple readers
    can query the database while the streaming pipeline is writing.
    Without WAL, writes lock the entire file — the dashboard would
    get "database is locked" errors during streaming ingestion.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # WAL: writers go to a separate log file, readers read the main file
    conn.execute("PRAGMA journal_mode=WAL")
    # NORMAL sync: safe with WAL, much faster than FULL
    conn.execute("PRAGMA synchronous=NORMAL")

    return conn


# ═══════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════

def create_schema():
    """
    Creates all four tables if they don't already exist.
    Safe to call on every run — IF NOT EXISTS means no data is lost
    if the DB already exists.

    Four tables, two stages:

    Stage 1 — Ingestion:
        raw_reads: every raw RFID row exactly as received from the stream,
                   before any transformation or filtering.
                   Simulates what a Kafka broker would durably store.
                   Allows full replay and reprocessing if algorithm changes.

    Stage 2 — Processing:
        sessions:  one summary row per sheet/test case.
        events:    confirmed pipeline events (ENTRY, EXIT, ZONE_TRANSITION,
                   SESSION_ENDED_INSIDE) produced by the detection algorithm.
        anomalies: flagged anomalies produced by the anomaly detection layer.
    """
    with get_connection() as conn:
        conn.executescript("""

            CREATE TABLE IF NOT EXISTS raw_reads (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,

                -- which sheet this row came from
                session_id           TEXT    NOT NULL,

                -- tag identity
                epc                  TEXT    NOT NULL,
                uid                  TEXT,

                -- location
                base_logical_device  TEXT    NOT NULL,
                direction            TEXT    NOT NULL,
                door                 TEXT    NOT NULL,
                antenna              INTEGER,

                -- signal
                rssi                 REAL    NOT NULL,

                -- time
                -- tag_time: full precision Unix ms — primary time source
                -- t0:       seconds since session start, derived from tag_time
                -- date_utc: human readable string, display only, NOT used for ordering
                tag_time             INTEGER NOT NULL,
                t0                   INTEGER NOT NULL,
                date_utc             TEXT,

                -- when this row was written to the DB
                ingested_at          TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_id          TEXT    PRIMARY KEY,
                epc                 TEXT    NOT NULL,
                session_date        TEXT,
                total_entries       INTEGER DEFAULT 0,
                total_exits         INTEGER DEFAULT 0,
                total_transitions   INTEGER DEFAULT 0,
                has_anomaly         INTEGER DEFAULT 0,
                ghost_read_ratio    REAL,
                entry_rssi_strength TEXT,
                ml_score            REAL,
                processed_at        TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT    NOT NULL REFERENCES sessions(session_id),
                epc         TEXT    NOT NULL,
                event_type  TEXT    NOT NULL,
                t0          INTEGER,
                door        TEXT,
                from_zone   TEXT,
                to_zone     TEXT,
                peak_rssi   REAL,
                dwell_time  INTEGER,
                note        TEXT
            );

            CREATE TABLE IF NOT EXISTS anomalies (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id    TEXT    NOT NULL REFERENCES sessions(session_id),
                epc           TEXT    NOT NULL,
                anomaly_type  TEXT    NOT NULL,
                t0            INTEGER,
                value         REAL,
                note          TEXT    NOT NULL,
                ml_score      REAL
            );

        """)

        # Migration: safely add new columns if upgrading from an older schema.
        # ALTER TABLE in SQLite does not support IF NOT EXISTS,
        # so we catch the OperationalError that fires when the column exists.
        migration_cols = [
            "ALTER TABLE sessions ADD COLUMN ghost_read_ratio    REAL",
            "ALTER TABLE sessions ADD COLUMN entry_rssi_strength TEXT",
            "ALTER TABLE sessions ADD COLUMN ml_score            REAL",
        ]
        for stmt in migration_cols:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                pass  # column already exists — safe to ignore


def update_session_quality(session_id: str, quality: Dict):
    """
    Updates the ghost_read_ratio and entry_rssi_strength columns
    on an existing session row.
    Called after compute_data_quality() in the processing stage.
    """
    with get_connection() as conn:
        conn.execute("""
            UPDATE sessions
            SET ghost_read_ratio    = ?,
                entry_rssi_strength = ?
            WHERE session_id = ?
        """, (
            quality.get("ghost_read_ratio"),
            quality.get("entry_rssi_strength"),
            session_id,
        ))


def update_session_ml(session_id: str, ml_score: float):
    """
    Updates the ml_score column on an existing session row.
    Called after run_ml() completes for all sessions.
    """
    with get_connection() as conn:
        conn.execute("""
            UPDATE sessions
            SET ml_score = ?
            WHERE session_id = ?
        """, (ml_score, session_id))

def session_to_datetime(session_id: str) -> str:
    """
    Converts a session_id to a human readable date string.

    Sheets 4-7 are named after the session start Unix ms timestamp
    (e.g. "1666806815845") — convert directly to UTC datetime.

    Sheets 1-3 are named like "16062021_1_9" — parse the date prefix.
    """
    try:
        ts = int(session_id.split("_")[0])
        if ts > 1_000_000_000_000:  # clearly a millisecond timestamp
            return datetime.fromtimestamp(
                ts / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, IndexError):
        pass

    try:
        date_part = session_id.split("_")[0]  # "16062021"
        return datetime.strptime(date_part, "%d%m%Y").strftime("%Y-%m-%d")
    except ValueError:
        return session_id  # fallback: return session name as-is


# ═══════════════════════════════════════════════════════════════
# STAGE 1 — INGESTION
# ═══════════════════════════════════════════════════════════════

def ingest_raw_reads(file_path: str, sheet_name: str):
    """
    Stage 1: streams raw RFID rows from Excel into the raw_reads table.
    No transformation, no filtering — rows are stored exactly as received.

    This simulates what a Kafka consumer writing to a sink database would do:
    durably persist every message before any processing happens.

    Clears existing rows for this session before inserting so re-running
    is safe and idempotent — you always get a clean slate.
    """
    from ingestion import stream_rfid_excel

    ingested_at = datetime.utcnow().isoformat()

    with get_connection() as conn:
        conn.execute(
            "DELETE FROM raw_reads WHERE session_id = ?", (sheet_name,)
        )
        for raw_row in stream_rfid_excel(file_path, sheet_name):
            conn.execute("""
                INSERT INTO raw_reads (
                    session_id, epc, uid, base_logical_device,
                    direction, door, antenna, rssi,
                    tag_time, t0, date_utc, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sheet_name,
                str(raw_row.get("epc", "")).strip(),
                str(raw_row.get("uid", "")).strip() or None,
                str(raw_row.get("baselogicaldevice", "")).strip(),
                str(raw_row.get("direction", "")).strip(),
                str(raw_row.get("door", "")).strip(),
                int(raw_row["antenna"]) if raw_row.get("antenna") else None,
                float(raw_row["rssi"]),
                int(float(str(raw_row["tagtime"]))),
                int(raw_row["T0"]),
                str(raw_row.get("dateutc", "")).strip() or None,
                ingested_at,
            ))


def get_raw_rows(session_id: str) -> List[Dict]:
    """
    Reads all raw rows for a session back from the DB.
    Sorted by t0 ASC, tag_time ASC — t0 is the primary sort,
    tag_time breaks ties within the same second since t0 is rounded.
    This guarantees the same chronological order the pipeline would
    see if reading directly from Excel.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM raw_reads
            WHERE session_id = ?
            ORDER BY t0 ASC, tag_time ASC
        """, (session_id,)).fetchall()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
# STAGE 2 — PROCESSED RESULTS
# ═══════════════════════════════════════════════════════════════

def insert_session(session_id: str, events: List[Dict], has_anomaly: bool):
    """
    Inserts or replaces a session row with aggregated event counts.
    OR REPLACE means re-running the pipeline updates existing rows safely.
    """
    epc = next(
        (e["epc"] for e in events if e.get("epc")), "unknown"
    )
    total_entries     = sum(1 for e in events if e["event"] == "ENTRY")
    total_exits       = sum(1 for e in events if e["event"] == "EXIT")
    total_transitions = sum(1 for e in events if e["event"] == "ZONE_TRANSITION")
    session_date      = session_to_datetime(session_id)

    with get_connection() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO sessions (
                session_id, epc, session_date,
                total_entries, total_exits, total_transitions,
                has_anomaly, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_id,
            epc,
            session_date,
            total_entries,
            total_exits,
            total_transitions,
            1 if has_anomaly else 0,
            datetime.utcnow().isoformat(),
        ))


def insert_events(session_id: str, events: List[Dict]):
    """
    Inserts all confirmed events for a session.
    Clears existing events for this session first — safe to re-run.
    """
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM events WHERE session_id = ?", (session_id,)
        )
        for e in events:
            conn.execute("""
                INSERT INTO events (
                    session_id, epc, event_type, t0, door,
                    from_zone, to_zone, peak_rssi, dwell_time, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id,
                e["epc"],
                e["event"],
                e.get("t0"),
                e.get("door"),
                e.get("from_zone"),
                e.get("to_zone"),
                e.get("peak_rssi"),
                e.get("dwell_time"),
                e.get("note"),
            ))


def insert_anomalies(session_id: str, anomalies: List[Dict]):
    """
    Inserts all detected anomalies for a session.
    Clears existing anomalies for this session first — safe to re-run.
    """
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM anomalies WHERE session_id = ?", (session_id,)
        )
        for a in anomalies:
            conn.execute("""
                INSERT INTO anomalies (
                    session_id, epc, anomaly_type, t0, value, note, ml_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id,
                a["epc"],
                a["type"],
                a.get("t0"),
                a.get("value"),
                a["note"],
                a.get("ml_score"),
            ))


# ═══════════════════════════════════════════════════════════════
# QUERIES — used by dashboard and analytics
# ═══════════════════════════════════════════════════════════════

def get_all_sessions() -> List[Dict]:
    """All sessions ordered by session date."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM sessions ORDER BY processed_at ASC
        """).fetchall()
    return [dict(r) for r in rows]


def get_all_events() -> List[Dict]:
    """All events across all sessions in chronological order."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM events ORDER BY session_id, t0 ASC
        """).fetchall()
    return [dict(r) for r in rows]


def get_recent_raw_reads(n: int = 20) -> List[Dict]:
    """
    Returns the most recent N raw reads across all sessions.
    Used by the dashboard live feed to show the raw RFID stream
    arriving row by row alongside the confirmed events.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT session_id, epc, direction, door,
                   rssi, t0, ingested_at
            FROM raw_reads
            ORDER BY id DESC
            LIMIT ?
        """, (n,)).fetchall()
    return [dict(r) for r in rows]


def get_events_for_session(session_id: str) -> List[Dict]:
    """All events for a specific session in chronological order."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM events
            WHERE session_id = ?
            ORDER BY t0 ASC
        """, (session_id,)).fetchall()
    return [dict(r) for r in rows]


def get_all_anomalies() -> List[Dict]:
    """All anomalies across all sessions."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT * FROM anomalies ORDER BY session_id, t0 ASC
        """).fetchall()
    return [dict(r) for r in rows]


def get_dwell_times() -> List[Dict]:
    """
    All EXIT events with a valid dwell_time.
    Used for dwell time distribution on the dashboard.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT session_id, epc, dwell_time
            FROM events
            WHERE event_type = 'EXIT'
            AND dwell_time IS NOT NULL
        """).fetchall()
    return [dict(r) for r in rows]


def get_door_usage() -> List[Dict]:
    """
    Entry and exit counts per door.
    Used for door usage breakdown on the dashboard.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT door, event_type, COUNT(*) as count
            FROM events
            WHERE event_type IN ('ENTRY', 'EXIT')
            AND door IS NOT NULL
            GROUP BY door, event_type
            ORDER BY door, event_type
        """).fetchall()
    return [dict(r) for r in rows]


def get_zone_transition_paths() -> List[Dict]:
    """
    All zone transition paths with counts.
    Used for movement pattern analysis on the dashboard.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT from_zone, to_zone, COUNT(*) as count
            FROM events
            WHERE event_type = 'ZONE_TRANSITION'
            GROUP BY from_zone, to_zone
            ORDER BY count DESC
        """).fetchall()
    return [dict(r) for r in rows]


def get_anomaly_summary() -> List[Dict]:
    """
    Anomaly counts grouped by type across all sessions.
    Used for the anomaly panel on the dashboard.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT anomaly_type, COUNT(*) as count
            FROM anomalies
            GROUP BY anomaly_type
            ORDER BY count DESC
        """).fetchall()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
# STREAMING — row-by-row writes for real-time mode
# ═══════════════════════════════════════════════════════════════

def clear_session_data(session_id: str):
    """
    Clears all data for a session before re-streaming it.
    Called at the start of each session in streaming mode so
    re-runs don't accumulate duplicate data.
    """
    with get_connection() as conn:
        conn.execute("DELETE FROM raw_reads  WHERE session_id = ?", (session_id,))
        conn.execute("DELETE FROM events     WHERE session_id = ?", (session_id,))
        conn.execute("DELETE FROM anomalies  WHERE session_id = ?", (session_id,))
        conn.execute("DELETE FROM sessions   WHERE session_id = ?", (session_id,))


def ingest_single_row(session_id: str, raw_row: Dict):
    """
    Writes a single raw RFID read to the database immediately.
    Called for every row as it arrives from stream_rfid_excel().

    Also creates the session row if it doesn't exist yet
    (INSERT OR IGNORE means safe to call on every row).

    This is the streaming equivalent of ingest_raw_reads() —
    instead of ingesting a whole sheet at once, we ingest one
    row at a time so the dashboard can see data arriving live.
    """
    epc          = str(raw_row.get("epc", "")).strip()
    session_date = session_to_datetime(session_id)

    with get_connection() as conn:
        # Create session row on first raw read for this session
        conn.execute("""
            INSERT OR IGNORE INTO sessions
            (session_id, epc, session_date, total_entries, total_exits,
             total_transitions, has_anomaly, processed_at)
            VALUES (?, ?, ?, 0, 0, 0, 0, ?)
        """, (session_id, epc, session_date, datetime.utcnow().isoformat()))

        # Insert the raw read
        conn.execute("""
            INSERT INTO raw_reads (
                session_id, epc, uid, base_logical_device,
                direction, door, antenna, rssi,
                tag_time, t0, date_utc, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_id,
            epc,
            str(raw_row.get("uid", "")).strip() or None,
            str(raw_row.get("baselogicaldevice", "")).strip(),
            str(raw_row.get("direction", "")).strip(),
            str(raw_row.get("door", "")).strip(),
            int(raw_row["antenna"]) if raw_row.get("antenna") else None,
            float(raw_row["rssi"]),
            int(float(str(raw_row["tagtime"]))),
            int(raw_row["T0"]),
            str(raw_row.get("dateutc", "")).strip() or None,
            datetime.utcnow().isoformat(),
        ))


def write_event_to_db(session_id: str, event: Dict):
    """
    Writes a single confirmed event to the database immediately.
    Called the moment the detection algorithm confirms an event
    during streaming — before the rest of the session is processed.

    Also updates the session row's event counts so the dashboard
    sees accurate totals as each event arrives.
    """
    event_type = event.get("event", event.get("event_type", ""))

    with get_connection() as conn:
        conn.execute("""
            INSERT INTO events (
                session_id, epc, event_type, t0, door,
                from_zone, to_zone, peak_rssi, dwell_time, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_id,
            event.get("epc"),
            event_type,
            event.get("t0"),
            event.get("door"),
            event.get("from_zone"),
            event.get("to_zone"),
            event.get("peak_rssi"),
            event.get("dwell_time"),
            event.get("note"),
        ))

        # Increment the appropriate session counter immediately
        if event_type == "ENTRY":
            conn.execute("""
                UPDATE sessions SET total_entries = total_entries + 1
                WHERE session_id = ?
            """, (session_id,))
        elif event_type == "EXIT":
            conn.execute("""
                UPDATE sessions SET total_exits = total_exits + 1
                WHERE session_id = ?
            """, (session_id,))
        elif event_type == "ZONE_TRANSITION":
            conn.execute("""
                UPDATE sessions SET total_transitions = total_transitions + 1
                WHERE session_id = ?
            """, (session_id,))