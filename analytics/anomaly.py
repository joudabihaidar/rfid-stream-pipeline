# analytics/anomaly.py
from typing import List, Dict
from collections import defaultdict

# ── Tunable thresholds ────────────────────────────────────────────────────────
SHORT_DWELL_THRESHOLD = 10   # seconds — below this is suspiciously short
LONG_DWELL_THRESHOLD  = 120  # seconds — above this is suspiciously long
RAPID_REENTRY_WINDOW  = 30   # seconds — re-entry within this of a prior exit


def detect_anomalies(events: List[Dict], raw_rows: List[Dict] = None) -> List[Dict]:
    """
    Detects behavioral anomalies from confirmed pipeline events,
    and optionally from raw RFID reads for signal-level anomalies.

    Two categories:

    Event-based (from events list):
        EXIT_WITHOUT_ENTRY     — exit with no prior confirmed entry
        SHORT_DWELL            — dwell time below SHORT_DWELL_THRESHOLD
        LONG_DWELL             — dwell time above LONG_DWELL_THRESHOLD
        RAPID_REENTRY          — entry within RAPID_REENTRY_WINDOW of prior exit
        SESSION_ENDED_INSIDE   — stream ended with person still inside
        SIMULTANEOUS_TRANSITIONS — two zone transitions at the same t0

    Raw-read-based (from raw_rows, if provided):
        OUT_READS_NO_IN_READS  — a door has Out reads but no In reads at all,
                                  suggesting an unmonitored entry point

    Returns a list of anomaly dicts ready to insert into the anomalies table.
    Each dict has: epc, type, t0, value, note.
    """
    if not events:
        return []

    anomalies  = []
    epc        = next((e["epc"] for e in events if e.get("epc")), "unknown")
    last_exit  = {}   # epc → t0 of most recent confirmed exit

    # ── 1. Event-based anomalies ──────────────────────────────────────────────

    transition_t0s = []   # collect for simultaneous check after the loop

    for e in events:
        event_type = e["event"]
        t0         = e.get("t0")
        epc_e      = e.get("epc", epc)

        # ── EXIT ──────────────────────────────────────────────────
        if event_type == "EXIT":
            dwell = e.get("dwell_time")

            if dwell is None:
                # Exit confirmed but no prior entry was detected
                anomalies.append({
                    "epc":   epc_e,
                    "type":  "EXIT_WITHOUT_ENTRY",
                    "t0":    t0,
                    "value": None,
                    "note":  (
                        f"Exit at t0={t0} on {e.get('door', '?')} "
                        f"with no prior confirmed entry"
                    ),
                })

            elif dwell < SHORT_DWELL_THRESHOLD:
                anomalies.append({
                    "epc":   epc_e,
                    "type":  "SHORT_DWELL",
                    "t0":    t0,
                    "value": dwell,
                    "note":  (
                        f"Dwell time {dwell}s is below the "
                        f"{SHORT_DWELL_THRESHOLD}s threshold"
                    ),
                })

            elif dwell > LONG_DWELL_THRESHOLD:
                anomalies.append({
                    "epc":   epc_e,
                    "type":  "LONG_DWELL",
                    "t0":    t0,
                    "value": dwell,
                    "note":  (
                        f"Dwell time {dwell}s exceeds the "
                        f"{LONG_DWELL_THRESHOLD}s threshold"
                    ),
                })

            # Record this exit time for rapid re-entry check
            if t0 is not None:
                last_exit[epc_e] = t0

        # ── ENTRY ─────────────────────────────────────────────────
        elif event_type == "ENTRY":
            if (
                epc_e in last_exit
                and t0 is not None
                and last_exit[epc_e] is not None
            ):
                gap = t0 - last_exit[epc_e]
                if gap <= RAPID_REENTRY_WINDOW:
                    anomalies.append({
                        "epc":   epc_e,
                        "type":  "RAPID_REENTRY",
                        "t0":    t0,
                        "value": gap,
                        "note":  (
                            f"Re-entry {gap}s after exit — "
                            f"possible tailgating or door hold"
                        ),
                    })

        # ── SESSION_ENDED_INSIDE ───────────────────────────────────
        elif event_type == "SESSION_ENDED_INSIDE":
            anomalies.append({
                "epc":   epc_e,
                "type":  "SESSION_ENDED_INSIDE",
                "t0":    None,
                "value": None,
                "note":  (
                    "Stream ended with person still inside — "
                    "possible overstay or missing exit data"
                ),
            })

        # ── ZONE_TRANSITION ────────────────────────────────────────
        elif event_type == "ZONE_TRANSITION":
            if t0 is not None:
                transition_t0s.append((t0, epc_e))

    # Simultaneous zone transitions — same t0 means physically impossible movement
    t0_groups = defaultdict(list)
    for t0_val, epc_val in transition_t0s:
        t0_groups[t0_val].append(epc_val)

    for t0_val, epcs in t0_groups.items():
        if len(epcs) > 1:
            anomalies.append({
                "epc":   epcs[0],
                "type":  "SIMULTANEOUS_TRANSITIONS",
                "t0":    t0_val,
                "value": float(len(epcs)),
                "note":  (
                    f"{len(epcs)} zone transitions fired at t0={t0_val} — "
                    f"person cannot be in two places simultaneously"
                ),
            })

    # ── 2. Raw-read anomalies ─────────────────────────────────────────────────

    if raw_rows:
        # Out reads with no In reads for the same door
        # A door that was used for exit but never for entry is suspicious —
        # either the entry was missed or it is an unmonitored access point
        door_directions = defaultdict(set)
        for r in raw_rows:
            door_directions[r["door"]].add(r["direction"])

        for door, directions in door_directions.items():
            if "Out" in directions and "In" not in directions:
                anomalies.append({
                    "epc":   epc,
                    "type":  "OUT_READS_NO_IN_READS",
                    "t0":    None,
                    "value": None,
                    "note":  (
                        f"Door {door} generated Out reads but no In reads — "
                        f"possible unmonitored entry point or reader malfunction"
                    ),
                })

    return anomalies