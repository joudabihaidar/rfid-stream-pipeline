from collections import defaultdict
from typing import Generator, Dict, Optional, List

def aggregate_window(buffer: List[Dict]) -> Dict:
    """
    Collapses a burst (list of raw rows for same device+direction
    within a time window) into one meaningful summary event.

    Key fields produced:
      count     — number of raw reads (used for noise filtering)
      peak_rssi — strongest signal seen (closest to 0 = person nearest reader)
      avg_rssi  — average signal (smooths out spikes)
      t0_start  — when the burst began
      t0_end    — when the burst ended
    """
    rssi_values = [r["rssi"] for r in buffer]
    return {
        "epc":       buffer[0]["epc"],
        "device":    buffer[0]["device"],
        "direction": buffer[0]["direction"],
        "door":      buffer[0]["door"],
        "t0_start":  buffer[0]["t0"],
        "t0_end":    buffer[-1]["t0"],
        "count":     len(buffer),
        "peak_rssi": max(rssi_values),
        "avg_rssi":  round(sum(rssi_values) / len(rssi_values), 2),
    }