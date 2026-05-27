
from __future__ import annotations

import sqlite3
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "rfid_events.db"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    print("=== raw_reads ===")
    r = conn.execute("SELECT COUNT(*) as n FROM raw_reads").fetchone()
    print(f"  total rows: {r['n']}")

    r = conn.execute(
        "SELECT session_id, COUNT(*) as n FROM raw_reads GROUP BY session_id"
    ).fetchall()
    for row in r:
        print(f"  {row['session_id']:<35} {row['n']} rows")

    print()
    print("=== sessions ===")
    r = conn.execute(
        "SELECT session_id, epc, session_date, total_entries, total_exits, total_transitions, has_anomaly FROM sessions"
    ).fetchall()
    for row in r:
        print(
            f"  {row['session_id']:<35} date={row['session_date']}  entries={row['total_entries']}  exits={row['total_exits']}  transitions={row['total_transitions']}  anomaly={row['has_anomaly']}"
        )

    print()
    print("=== events ===")
    r = conn.execute("SELECT COUNT(*) as n FROM events").fetchone()
    print(f"  total events: {r['n']}")
    r = conn.execute(
        "SELECT event_type, COUNT(*) as n FROM events GROUP BY event_type ORDER BY n DESC"
    ).fetchall()
    for row in r:
        print(f"  {row['event_type']:<25} {row['n']}")

    print()
    print("=== anomalies ===")
    r = conn.execute("SELECT COUNT(*) as n FROM anomalies").fetchone()
    print(
        f"  total anomalies: {r['n']} (empty until analytics/anomaly.py is wired in)"
    )

    conn.close()


if __name__ == "__main__":
    main()
