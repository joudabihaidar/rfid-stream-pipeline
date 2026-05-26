from typing import Dict, Optional, List

# ═══════════════════════════════════════════════════════════════
# 4. BUILDING-LEVEL TRACKER (Entry / Exit)
# ═══════════════════════════════════════════════════════════════

class EPCTracker:
    """
    Tracks whether a single person (EPC) is INSIDE or OUTSIDE the building.

    State machine:
        UNKNOWN → INSIDE  (first confirmed entry)
        INSIDE  → OUTSIDE (confirmed exit)
        OUTSIDE → INSIDE  (re-entry)

    Three filters gate every state change:
        Filter 1 — noise:  burst count must meet minimum threshold
        Filter 2 — ghost:  weak Out reads while inside are suppressed
        Filter 3 — duplicate: ignores events that wouldn't change state

    Ghost suppression uses BOTH time AND RSSI (change 1 + 2):
        If an Out burst is followed by an In burst within PENDING_EXIT_TIMEOUT
        seconds AND the Out signal was weaker or equal to the In signal,
        the Out burst is treated as a ghost read and discarded.
        If the Out signal was stronger, the person was genuinely near the
        exit — the exit is confirmed before the entry.
    """

    MIN_COUNT            = 3    # fewer reads than this = noise, ignore
    RSSI_GHOST_THRESHOLD = -72  # weaker than this = ghost, ignore immediately
    PENDING_EXIT_TIMEOUT = 10   # seconds (change 1: reduced from 15)

    def __init__(self, epc: str):
        self.epc          = epc
        self.state        = "UNKNOWN"
        self.events       = []
        self.pending_exit = None  # holds unconfirmed Out burst
        self.entry_t0     = None  # used to compute dwell time on exit

    def process_event(self, agg: Dict) -> Optional[Dict]:
        direction = agg["direction"]
        peak_rssi = agg["peak_rssi"]
        count     = agg["count"]
        t0        = agg["t0_start"]

        # ── Filter 1: noise suppression ──────────────────────────
        if count < self.MIN_COUNT:
            return None

        # ── Filter 2: immediate ghost suppression ─────────────────
        # Out read while inside with a very weak signal = bleed-through wall
        if direction == "Out" and self.state in ("INSIDE", "UNKNOWN"):
            if peak_rssi < self.RSSI_GHOST_THRESHOLD:
                return None

        # ── Filter 3: duplicate state ─────────────────────────────
        if direction == "In"  and self.state == "INSIDE":  return None
        if direction == "Out" and self.state == "OUTSIDE": return None

        # ── Pending exit: hold Out bursts, don't confirm immediately ──
        if direction == "Out":
            self.pending_exit = agg
            return None

        # ── direction == "In" from here ───────────────────────────
        if self.pending_exit is not None:
            gap           = t0 - self.pending_exit["t0_start"]
            out_rssi      = self.pending_exit["peak_rssi"]
            in_rssi       = peak_rssi

            if gap <= self.PENDING_EXIT_TIMEOUT:
                # change 2: combine time AND RSSI for ghost decision
                if out_rssi <= in_rssi:
                    # Out signal weaker or equal → person walking toward inside
                    # → Out was a ghost, discard it
                    self.pending_exit = None
                else:
                    # Out signal stronger → person was genuinely near exit first
                    # → real exit followed by quick re-entry, confirm both
                    self._confirm_exit(self.pending_exit)
                    self.pending_exit = None
            else:
                # Gap too large → exit was real regardless of RSSI
                self._confirm_exit(self.pending_exit)
                self.pending_exit = None

        # ── Confirm entry ─────────────────────────────────────────
        self.state    = "INSIDE"
        self.entry_t0 = t0
        event = {
            "epc":       self.epc,
            "event":     "ENTRY",
            "door":      agg["door"],
            "t0":        t0,
            "peak_rssi": peak_rssi,
            "count":     count,
        }
        self.events.append(event)
        return event

    def _confirm_exit(self, agg: Dict):
        """Confirms an exit event and computes dwell time (change 5)."""
        self.state    = "OUTSIDE"
        dwell_time    = agg["t0_start"] - self.entry_t0 if self.entry_t0 is not None else None
        event = {
            "epc":        self.epc,
            "event":      "EXIT",
            "door":       agg["door"],
            "t0":         agg["t0_start"],
            "peak_rssi":  agg["peak_rssi"],
            "count":      agg["count"],
            "dwell_time": dwell_time,   # seconds spent inside (change 5)
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
# 5. MOVEMENT TRACKER (Zone transitions inside the building)
# ═══════════════════════════════════════════════════════════════

class MovementTracker:
    """
    Tracks a person's movement between checkpoints while INSIDE the building.
    Only activates when EPCTracker state is INSIDE.

    Produces ZONE_TRANSITION events when the person moves from one
    inside reader to another — giving a trail of their path through
    the building during their visit.

    Note: Out reads are ignored here — those belong to EPCTracker.
    """

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

        new_zone = agg["device"]

        # First zone detected after entry — just set it, no transition yet
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
            "t0":        agg["t0_start"],
            "peak_rssi": agg["peak_rssi"],
        }
        self.transitions.append(transition)
        self.current_zone = new_zone
        return transition

    def reset(self):
        """Call when person exits — clears their position for next entry."""
        self.current_zone = None
