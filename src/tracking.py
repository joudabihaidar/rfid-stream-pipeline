from typing import Dict, Optional, List


# ═══════════════════════════════════════════════════════════════
# BUILDING-LEVEL TRACKER (Entry / Exit)
# ═══════════════════════════════════════════════════════════════

class EPCTracker:
    """
    Tracks whether a single person (EPC) is INSIDE or OUTSIDE the building.

    State machine:
        UNKNOWN → INSIDE  (first confirmed entry)
        INSIDE  → OUTSIDE (confirmed exit)
        OUTSIDE → INSIDE  (re-entry)

    Three filters gate every state change:
        Filter 1 — noise:     burst count must meet minimum threshold
        Filter 2 — ghost:     weak Out reads while inside are suppressed
        Filter 3 — duplicate: ignores events that wouldn't change state

    Ghost suppression uses BOTH time AND RSSI:
        If an Out burst is followed by an In burst within PENDING_EXIT_TIMEOUT
        seconds AND the Out signal was weaker or equal to the In signal,
        the Out burst is treated as a ghost read and discarded.
        If the Out signal was stronger, the person was genuinely near the
        exit — the exit is confirmed before the entry.

    process_event() returns a LIST of confirmed events, not a single event.
    This is necessary because one burst can produce both an EXIT and an ENTRY
    (real exit followed by quick re-entry), and both must be returned.
    """

    MIN_COUNT            = 3
    RSSI_GHOST_THRESHOLD = -72
    PENDING_EXIT_TIMEOUT = 10   # seconds

    def __init__(self, epc: str):
        self.epc          = epc
        self.state        = "UNKNOWN"
        self.events       = []
        self.pending_exit = None
        self.entry_t0     = None

    def process_event(self, agg: Dict) -> List[Dict]:
        """
        Processes one aggregated burst.
        Returns a list of confirmed events — empty if filtered out,
        [ENTRY] normally, or [EXIT, ENTRY] when a real exit precedes re-entry.
        """
        direction = agg["direction"]
        peak_rssi = agg["peak_rssi"]
        count     = agg["count"]
        t0        = agg["t0_start"]
        confirmed = []

        # ── Filter 1: noise suppression ───────────────────────────
        # Too few reads — not a real crossing event, discard
        if count < self.MIN_COUNT:
            return []

        # ── Filter 2: immediate ghost suppression ──────────────────
        # Out read while inside/unknown with a very weak signal
        # = tag bleeding through the wall, not a real exit
        if direction == "Out" and self.state in ("INSIDE", "UNKNOWN"):
            if peak_rssi < self.RSSI_GHOST_THRESHOLD:
                return []

        # ── Filter 3: duplicate state ──────────────────────────────
        # Event wouldn't change state — nothing to do
        if direction == "In"  and self.state == "INSIDE":  return []
        if direction == "Out" and self.state == "OUTSIDE": return []

        # ── Hold Out bursts as pending — don't confirm immediately ─
        # We wait to see if an In burst follows (ghost check)
        if direction == "Out":
            self.pending_exit = agg
            return []

        # ── direction == "In" from here ────────────────────────────
        if self.pending_exit is not None:
            gap      = t0 - self.pending_exit["t0_start"]
            out_rssi = self.pending_exit["peak_rssi"]
            in_rssi  = peak_rssi

            if gap <= self.PENDING_EXIT_TIMEOUT:
                if out_rssi <= in_rssi:
                    # Out signal weaker or equal → person walking toward inside
                    # → Out was a ghost, discard it
                    self.pending_exit = None
                else:
                    # Out signal stronger → person was genuinely near exit first
                    # → real exit followed by quick re-entry, confirm exit first
                    exit_event = self._confirm_exit(self.pending_exit)
                    confirmed.append(exit_event)
                    self.pending_exit = None
            else:
                # Gap too large → exit was real regardless of RSSI
                exit_event = self._confirm_exit(self.pending_exit)
                confirmed.append(exit_event)
                self.pending_exit = None

        # ── Confirm entry ──────────────────────────────────────────
        self.state    = "INSIDE"
        self.entry_t0 = t0
        entry_event = {
            "epc":       self.epc,
            "event":     "ENTRY",
            "door":      agg["door"],
            "t0":        t0,
            "peak_rssi": peak_rssi,
            "count":     count,
        }
        self.events.append(entry_event)
        confirmed.append(entry_event)
        return confirmed

    def _confirm_exit(self, agg: Dict) -> Dict:
        """
        Confirms an exit event and computes dwell time.
        Always returns an event — never discards.
        dwell_time is None when there was no prior confirmed entry,
        which is itself an anomaly signal for the detection layer.
        """
        self.state = "OUTSIDE"
        dwell_time = agg["t0_end"] - self.entry_t0 if self.entry_t0 is not None else None
        event = {
            "epc":        self.epc,
            "event":      "EXIT",
            "door":       agg["door"],
            "t0":         agg["t0_end"],
            "peak_rssi":  agg["peak_rssi"],
            "count":      agg["count"],
            "dwell_time": dwell_time,
        }
        self.events.append(event)
        self.entry_t0 = None
        return event

    def flush_pending_exit(self) -> Optional[Dict]:
        """
        Called at end of stream.
        If an Out burst was pending and never followed by an In,
        the person genuinely left — confirm the exit.
        """
        if self.pending_exit is not None:
            agg = self.pending_exit
            self.pending_exit = None
            if agg["count"] >= self.MIN_COUNT:
                return self._confirm_exit(agg)
        return None


# ═══════════════════════════════════════════════════════════════
# MOVEMENT TRACKER (Zone transitions inside the building)
# ═══════════════════════════════════════════════════════════════

class MovementTracker:
    """
    Tracks a person's movement between checkpoints while INSIDE the building.
    Only activates when EPCTracker state is INSIDE.

    Produces ZONE_TRANSITION events when the person moves from one
    inside reader to another.

    Uses t0_end (last row of burst) as the transition timestamp — not
    t0_start — because a burst may begin accumulating before the building-
    level entry is confirmed. Using t0_end ensures the transition timestamp
    is always within the confirmed INSIDE period.
    """

    MIN_COUNT = 3

    def __init__(self, epc: str):
        self.epc          = epc
        self.current_zone = None
        self.transitions  = []

    def process_event(self, agg: Dict, building_state: str) -> Optional[Dict]:
        # Only track movement when person is confirmed inside
        if building_state != "INSIDE":
            return None

        # Out reads belong to the building tracker, not here
        if agg["direction"] == "Out":
            return None

        # Same noise filter as EPCTracker
        if agg["count"] < self.MIN_COUNT:
            return None

        new_zone = agg["device"]

        # First zone detected after entry — set it, no transition yet
        if self.current_zone is None:
            self.current_zone = new_zone
            return None

        # Still at the same checkpoint — no transition
        if new_zone == self.current_zone:
            return None

        # Genuine zone transition
        transition = {
            "epc":       self.epc,
            "event":     "ZONE_TRANSITION",
            "from_zone": self.current_zone,
            "to_zone":   new_zone,
            "t0":        agg["t0_end"],    # use t0_end, not t0_start
            "peak_rssi": agg["peak_rssi"],
        }
        self.transitions.append(transition)
        self.current_zone = new_zone
        return transition

    def reset(self):
        """Call when person exits — clears position for next entry."""
        self.current_zone = None