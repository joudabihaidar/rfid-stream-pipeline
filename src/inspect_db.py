from __future__ import annotations

import sqlite3
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "rfid_events.db"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    print("=== sessions ===")
    rows = conn.execute(
        "SELECT session_id, has_anomaly, ghost_read_ratio, entry_rssi_strength FROM sessions"
    ).fetchall()
    for r in rows:
        print(
            f"  {r['session_id']:<35} has_anomaly={r['has_anomaly']}  "
            f"ghost_ratio={r['ghost_read_ratio']}  rssi_strength={r['entry_rssi_strength']}"
        )

    print()
    print("=== anomalies ===")
    rows = conn.execute(
        "SELECT session_id, anomaly_type, t0, value, note FROM anomalies ORDER BY session_id"
    ).fetchall()
    if not rows:
        print("  empty")
    for r in rows:
        print(
            f"  {r['session_id']:<35} {r['anomaly_type']:<30} "
            f"t0={str(r['t0']):<6} value={r['value']}"
        )
        print(f"    note: {r['note']}")

    print()
    print("=== anomaly summary ===")
    rows = conn.execute(
        "SELECT anomaly_type, COUNT(*) as n FROM anomalies GROUP BY anomaly_type ORDER BY n DESC"
    ).fetchall()
    for r in rows:
        print(f"  {r['anomaly_type']:<30} count={r['n']}")

    conn.close()


if __name__ == "__main__":
    main()
