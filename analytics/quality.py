# analytics/quality.py
from typing import List, Dict, Optional

# ── Tunable thresholds ────────────────────────────────────────────────────────
STRONG_RSSI_THRESHOLD   = -65  # peak_rssi >= this → strong entry signal
MODERATE_RSSI_THRESHOLD = -70  # peak_rssi >= this → moderate, else weak
ENTRY_BURST_WINDOW      = 5    # seconds around entry t0 to collect In reads


def compute_data_quality(events: List[Dict], raw_rows: List[Dict]) -> Dict:
    """
    Computes signal-level data quality metrics for a session.

    These are NOT anomalies — they are diagnostic scores that tell a data
    scientist how much to trust the events produced by the pipeline.

    Two metrics:

    ghost_read_ratio (float 0.0-1.0):
        Proportion of total reads that were Out reads firing while the
        person was confirmed INSIDE. A high ratio means the outside reader
        was consistently picking up the tag through the wall — the session
        data is noisier and the events less reliable.

    entry_rssi_strength ("strong" / "moderate" / "weak" / None):
        How strong the signal was during the entry burst. A weak entry
        means the person was far from the reader when entry was confirmed —
        the event is technically correct but less physically certain.

    Returns a dict ready to be stored on the sessions row.
    """
    result = {
        "ghost_read_ratio":    None,
        "entry_rssi_strength": None,
    }

    if not raw_rows or not events:
        return result

    # Find first confirmed entry t0 and last valid exit t0
    entry_t0 = None
    exit_t0  = None
    for e in events:
        if e["event"] == "ENTRY" and entry_t0 is None:
            entry_t0 = e.get("t0")
        if e["event"] == "EXIT" and e.get("dwell_time") is not None:
            exit_t0 = e.get("t0")

    # ── Ghost read ratio ──────────────────────────────────────────────────────
    # Only meaningful once an entry is confirmed — before that, Out reads
    # are just part of the pre-entry signal, not ghosts
    if entry_t0 is not None:
        total_reads = len(raw_rows)

        out_reads_during_inside = sum(
            1 for r in raw_rows
            if r["direction"] == "Out"
            and int(r["t0"]) >= entry_t0
            and (exit_t0 is None or int(r["t0"]) < exit_t0)
        )

        result["ghost_read_ratio"] = (
            round(out_reads_during_inside / total_reads, 3)
            if total_reads > 0 else 0.0
        )

    # ── Entry RSSI strength ───────────────────────────────────────────────────
    # Collect all In reads within ENTRY_BURST_WINDOW seconds of the entry t0
    if entry_t0 is not None:
        entry_in_reads = [
            r for r in raw_rows
            if r["direction"] == "In"
            and abs(int(r["t0"]) - entry_t0) <= ENTRY_BURST_WINDOW
        ]

        if entry_in_reads:
            peak = max(float(r["rssi"]) for r in entry_in_reads)

            if peak >= STRONG_RSSI_THRESHOLD:
                result["entry_rssi_strength"] = "strong"
            elif peak >= MODERATE_RSSI_THRESHOLD:
                result["entry_rssi_strength"] = "moderate"
            else:
                result["entry_rssi_strength"] = "weak"

    return result