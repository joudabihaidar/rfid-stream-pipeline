from collections import defaultdict
from typing import Dict, Iterator, List

from aggregation import aggregate_window
from ingestion import stream_rfid_excel
from preprocessing import preprocess_row
from tracking import EPCTracker, MovementTracker

# ═══════════════════════════════════════════════════════════════
# STALE BUFFER FLUSHER
# ═══════════════════════════════════════════════════════════════

WINDOW_SIZE = 3  # seconds of silence before a burst is considered closed

def flush_stale_buffers(
    window_buffer: dict,
    epc_trackers:  dict,
    mov_trackers:  dict,
    current_t0:    int
) -> List[Dict]:
    """
    Checks every open buffer. If its last row is older than WINDOW_SIZE
    seconds, the burst has ended — aggregate it and send to both trackers.

    Uses session-timeout model: gap measured from LAST row in buffer.
    Sorts flushed bursts by t0_start — earlier Out bursts must be evaluated
    before later In bursts for ghost suppression to work correctly.

    process_event returns a LIST of events ([EXIT, ENTRY] when a real exit
    is immediately followed by re-entry). We iterate over that list.
    """
    to_flush = []
    for key in list(window_buffer.keys()):
        buf = window_buffer[key]
        if buf and (current_t0 - buf[-1]["t0"]) > WINDOW_SIZE:
            to_flush.append(aggregate_window(buf))
            del window_buffer[key]

    # Chronological order — required for pending exit ghost logic
    to_flush.sort(key=lambda x: x["t0_start"])

    confirmed = []
    for agg in to_flush:
        epc = agg["epc"]

        building_events = epc_trackers[epc].process_event(agg)
        for building_event in building_events:
            confirmed.append(building_event)
            if building_event["event"] == "EXIT":
                mov_trackers[epc].reset()

        # Movement reads state AFTER building events update it
        # If EXIT fired above, state=OUTSIDE → movement blocked automatically
        movement_event = mov_trackers[epc].process_event(
            agg, epc_trackers[epc].state
        )
        if movement_event:
            confirmed.append(movement_event)

    return confirmed


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_pipeline(file_path: str, sheet_name: str) -> List[Dict]:
    """
    Full pipeline for one Excel sheet (one test case).
    Reads directly from Excel via stream_rfid_excel.

    Returns all confirmed events (ENTRY, EXIT, ZONE_TRANSITION,
    SESSION_ENDED_INSIDE) sorted chronologically.

    All events are surfaced as-is — including exits before entries,
    zone transitions before entries, and zone transitions after exits.
    These are data anomalies and are preserved intentionally so the
    anomaly detection layer can flag and analyse them.

    Reset between sheets by calling run_pipeline() fresh for each —
    trackers and buffers are local to each call.
    """
    def rows():
        for raw_row in stream_rfid_excel(file_path, sheet_name):
            row = preprocess_row(raw_row)
            if row is not None:
                yield row

    return _process_rows(rows())


def run_pipeline_from_rows(db_rows: List[Dict]) -> List[Dict]:
    """
    Same pipeline logic as run_pipeline but reads from pre-loaded
    rows sourced from the raw_reads database table (Stage 2 processing).

    This is called after ingest_raw_reads() has already written raw
    rows to the DB. The pipeline logic is identical — only the data
    source changes. This separation means the algorithm can be re-run
    on stored data without touching the Excel file again.

    db_rows must be sorted by t0 ASC, tag_time ASC before calling —
    get_raw_rows() in database.py guarantees this.
    """
    def rows():
        for r in db_rows:
            # DB rows are already clean — map column names directly
            # without going through preprocess_row
            yield {
                "epc":       r["epc"],
                "device":    r["base_logical_device"],
                "direction": r["direction"],
                "door":      r["door"],
                "rssi":      float(r["rssi"]),
                "t0":        int(r["t0"]),
            }

    return _process_rows(rows())


def _process_rows(rows: Iterator[Dict]) -> List[Dict]:
    """
    Core pipeline logic. Accepts an iterator of preprocessed row dicts
    and returns confirmed events. Called by both run_pipeline and
    run_pipeline_from_rows — the only difference between them is
    where the rows come from (Excel vs database).
    """
    epc_trackers:  Dict[str, EPCTracker]      = {}
    mov_trackers:  Dict[str, MovementTracker]  = {}
    window_buffer: Dict[tuple, list]           = defaultdict(list)
    all_events:    List[Dict]                  = []

    for row in rows:

        epc = row["epc"]
        key = (epc, row["device"], row["direction"])

        if epc not in epc_trackers:
            epc_trackers[epc] = EPCTracker(epc)
            mov_trackers[epc] = MovementTracker(epc)

        events = flush_stale_buffers(
            window_buffer, epc_trackers, mov_trackers, row["t0"]
        )
        all_events.extend(events)

        window_buffer[key].append(row)

    # End of stream: flush all remaining open buffers in chronological order
    remaining = sorted(
        [aggregate_window(buf) for buf in window_buffer.values() if buf],
        key=lambda x: x["t0_start"]
    )
    for agg in remaining:
        epc = agg["epc"]

        building_events = epc_trackers[epc].process_event(agg)
        for building_event in building_events:
            all_events.append(building_event)
            if building_event["event"] == "EXIT":
                mov_trackers[epc].reset()

        movement_event = mov_trackers[epc].process_event(
            agg, epc_trackers[epc].state
        )
        if movement_event:
            all_events.append(movement_event)

    # Confirm any exits still pending at end of stream
    for epc, tracker in epc_trackers.items():
        exit_event = tracker.flush_pending_exit()
        if exit_event:
            all_events.append(exit_event)

    # Flag sessions that ended without a confirmed exit.
    # Covers INSIDE (entry confirmed, no exit) and UNKNOWN (only exits
    # recorded with no entry — exit events still surface with dwell_time=None).
    for epc, tracker in epc_trackers.items():
        if tracker.state in ("INSIDE", "UNKNOWN"):
            all_events.append({
                "epc":   epc,
                "event": "SESSION_ENDED_INSIDE",
                "t0":    None,
                "note":  "Stream ended before exit was detected — possible data gap or anomaly"
            })

    # Sort all events chronologically by t0.
    # SESSION_ENDED_INSIDE has t0=None — pushed to the end.
    all_events.sort(key=lambda e: e["t0"] if e["t0"] is not None else float("inf"))

    return all_events