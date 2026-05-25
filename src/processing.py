from typing import Generator, Dict, List, Optional


def parse_tag_time(value: str) -> int:
    """CSV exports TagTime in scientific notation (e.g. 1.62388E+12)."""
    return int(float(value))


def aggregate_window(buffer: List[Dict]) -> Dict:
    """
    Takes a list of preprocessed rows for the exact same burst
    and collapses them into a single summarized event frame.
    """
    # 1. Keep converting RSSI to integers to find the true physical peak
    rssi_values = [int(r["RSSI"]) for r in buffer]
    
    # 2. Extract structural components using clean, relative 'T0' seconds
    return {
        "uid":        buffer[0]["UID"],
        "device":     buffer[0]["BaseLogicalDevice"],
        "direction":  buffer[0]["Direction"],
        "door":       buffer[0]["Door"],
        "t0_start":   int(buffer[0]["T0"]),   # Changed from TagTime to T0
        "t0_end":     int(buffer[-1]["T0"]),  # Changed from TagTime to T0
        "count":      len(buffer),
        "peak_rssi":  max(rssi_values),       # Highest value (least negative)
        "avg_rssi":   sum(rssi_values) / len(rssi_values),
    }

def chunk_rfid_stream(raw_stream, max_gap_seconds: int = 3):
    active_bursts = {}
    
    for row in raw_stream:
        uid = row["UID"]
        device = row["BaseLogicalDevice"]
        direction = row["Direction"]
        
        # FIX: Use the clean, relative 'T0' column which is explicitly in seconds
        current_time = int(row["T0"])
        
        burst_key = (uid, device, direction)
        
        if burst_key in active_bursts:
            buffer = active_bursts[burst_key]
            last_time = int(buffer[-1]["T0"]) # Read historical T0
            
            if current_time - last_time > max_gap_seconds:
                yield aggregate_window(buffer)
                active_bursts[burst_key] = [row]
            else:
                buffer.append(row)
        else:
            active_bursts[burst_key] = [row]
            
    for burst_key, remaining_buffer in active_bursts.items():
        if remaining_buffer:
            yield aggregate_window(remaining_buffer)