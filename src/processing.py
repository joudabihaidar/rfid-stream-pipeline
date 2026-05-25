from typing import Generator, Dict, List, Optional


def parse_tag_time(value: str) -> int:
    """CSV exports TagTime in scientific notation (e.g. 1.62388E+12)."""
    return int(float(value))


def aggregate_window(buffer: List[Dict]) -> Dict:
    """
    Takes a list of preprocessed rows for the exact same burst
    and collapses them into a single summarized event frame.
    """
    rssi_values = [int(r["RSSI"]) for r in buffer]
    
    # Extract structural components from our parsed fields
    # (Using the underlying columns from your Excel schema)
    return {
        "uid":        buffer[0]["UID"],
        "device":     buffer[0]["BaseLogicalDevice"],
        "direction":  buffer[0]["Direction"],
        "door":       buffer[0]["Door"],
        "t0_start":   parse_tag_time(buffer[0]["TagTime"]),
        "t0_end":     parse_tag_time(buffer[-1]["TagTime"]),
        "count":      len(buffer),
        "peak_rssi":  max(rssi_values),  # Least negative = physically strongest
        "avg_rssi":   sum(rssi_values) / len(rssi_values),
    }

def chunk_rfid_stream(
    raw_stream: Generator[Dict, None, None], 
    max_gap_seconds: int = 3
) -> Generator[Dict, None, None]:
    """
    Monitors the streaming generator. Groups rows into independent bursts 
    based on matching: UID, Device, Direction, and a maximum time quiet gap.
    Yields an aggregated event frame the moment a burst ends.
    """
    # Key: (UID, Device, Direction) -> Value: List of raw row dictionaries
    active_bursts: Dict[tuple, List[Dict]] = {}
    
    for row in raw_stream:
        # 1. Create the unique routing coordinates for this row
        uid = row["UID"]
        device = row["BaseLogicalDevice"]
        direction = row["Direction"]
        current_time = parse_tag_time(row["TagTime"])
        
        burst_key = (uid, device, direction)
        
        # 2. Check if this tracking coordinate already has a live buffer active
        if burst_key in active_bursts:
            buffer = active_bursts[burst_key]
            last_time = parse_tag_time(buffer[-1]["TagTime"])
            
            # Criterion 4 Check: Has a silence gap occurred?
            if current_time - last_time > max_gap_seconds:
                # The old burst is dead. Aggregate and emit it!
                yield aggregate_window(buffer)
                
                # Reset the coordinate buffer with the current row starting a new burst
                active_bursts[burst_key] = [row]
            else:
                # Still within the time-gap cluster! Append row to active burst
                buffer.append(row)
        else:
            # First time seeing this tracking coordinate combination. Start a buffer.
            active_bursts[burst_key] = [row]
            
    # --- STREAM END CLEANUP ---
    # When the CSV runs completely out of rows, flush any remaining bursts hanging in RAM
    for burst_key, remaining_buffer in active_bursts.items():
        if remaining_buffer:
            yield aggregate_window(remaining_buffer)