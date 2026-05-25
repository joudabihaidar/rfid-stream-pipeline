from typing import Dict, Optional, List

class UIDTracker:
    """
    Tracks the real-time physical state of a single unique badge (UID).
    States: 'UNKNOWN', 'INSIDE', 'OUTSIDE'
    """
    RSSI_GHOST_THRESHOLD = -72  # Signal weaker than this = noise/bleed
    MIN_COUNT = 3               # Fewer hits than this = fleeting noise
    EXIT_GRACE_PERIOD = 15      # Seconds to wait before confirming an exit

    def __init__(self, uid: str):
        self.uid = uid
        self.state = "UNKNOWN"
        self.events_history: List[Dict] = []
        self.pending_exit: Optional[Dict] = None

    def process_event(self, agg_event: Dict, current_stream_time: int) -> Optional[Dict]:
        """
        Processes a single aggregated burst event frame.
        Returns a confirmed transaction dictionary ('ENTRY' or 'EXIT') or None.
        """
        direction = agg_event["direction"]
        peak_rssi = agg_event["peak_rssi"]
        count = agg_event["count"]
        t0 = agg_event["t0_start"]

        # --- RULE 1: TIME-BASED EVICTION ---
        # Before evaluating the new frame, check if a held exit has aged out
        if self.pending_exit is not None:
            time_gap = current_stream_time - self.pending_exit["t0_start"]
            if time_gap > self.EXIT_GRACE_PERIOD:
                # 15+ seconds of quiet passed. The exit was real!
                confirmed_exit = self._commit_exit()
                
                # If the new event that woke us up is an 'In', process it next
                if direction == "In":
                    self.state = "INSIDE"
                    confirmed_entry = self._commit_entry(agg_event)
                    return confirmed_entry # In a stream, you'd emit both, let's return this entry
                
                return confirmed_exit

        # --- RULE 2: NOISE & GHOST FILTERS ---
        if count < self.MIN_COUNT:
            return None

        if direction == "Out" and self.state in ("INSIDE", "UNKNOWN"):
            if peak_rssi < self.RSSI_GHOST_THRESHOLD:
                # Suppress signal bleed through office walls
                return None

        # --- RULE 3: DUPLICATE STATE PROTECTION ---
        if direction == "In" and self.state == "INSIDE":
            return None
        if direction == "Out" and self.state == "OUTSIDE":
            return None

        # --- RULE 4: TRANSITION LOGIC ---
        if direction == "Out" and self.state in ("UNKNOWN", "INSIDE"):
            # Put the exit in purgatory to see if they immediately step back in
            self.pending_exit = agg_event
            return None

        if direction == "In":
            if self.pending_exit is not None:
                # An 'In' burst arrived quickly! The 'Out' was just a ghost read.
                self.pending_exit = None 
            
            self.state = "INSIDE"
            return self._commit_entry(agg_event)

        return None

    def flush_final_exit(self) -> Optional[Dict]:
        """Forcibly flushes any remaining pending exit when the data file ends."""
        if self.pending_exit:
            return self._commit_exit()
        return None

    def _commit_entry(self, agg_event: Dict) -> Dict:
        entry_record = {
            "uid": self.uid,
            "event": "ENTRY",
            "door": agg_event["door"],
            "timestamp": agg_event["t0_start"],
            "peak_rssi": agg_event["peak_rssi"]
        }
        self.events_history.append(entry_record)
        return entry_record

    def _commit_exit(self) -> Dict:
        exit_record = {
            "uid": self.uid,
            "event": "EXIT",
            "door": self.pending_exit["door"],
            "timestamp": self.pending_exit["t0_start"],
            "peak_rssi": self.pending_exit["peak_rssi"]
        }
        self.events_history.append(exit_record)
        self.state = "OUTSIDE"
        self.pending_exit = None
        return exit_record