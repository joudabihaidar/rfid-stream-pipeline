import csv
from typing import List, Dict
from typing import Generator, Dict, Optional, List

# Mapping from lowercase (sheets 4-7) to canonical column names
COLUMN_MAP = {
    "id":                "ID",
    "transactionid":     "TransactionID",
    "epc":               "EPC",
    "uid":               "UID",
    "baselogicaldevice": "BaseLogicalDevice",
    "direction":         "Direction",
    "door":              "Door",
    "antenna":           "Antenna",
    "rssi":              "RSSI",
    "tagtime":           "TagTime",
    "dateutc":           "DateUTC",
    "t0":                "T0",           # may not exist in sheets 4-7
}

def normalize_columns(row: Dict[str, str]) -> Dict[str, str]:
    """
    Unifies column names across all sheets.
    Sheets 4-7 use lowercase — map them to the canonical casing.
    """
    return {
        COLUMN_MAP.get(k.strip().lower(), k.strip()): v
        for k, v in row.items()
    }

def derive_t0(tag_time_ms: int, session_start_ms: int) -> int:
    """
    Derives T0 (seconds since session start) from TagTime.
    Used for sheets 4-7 that don't have a T0 column.

    TagTime is Unix timestamp in milliseconds.
    session_start_ms is the TagTime of the first row in the sheet.
    """
    return round((tag_time_ms - session_start_ms) / 1000)

def parse_tagtime(raw: str) -> int:
    """
    Handles both proper integers ('1623878674398')
    and scientific notation strings ('1.66794E+12').
    """
    return int(float(raw))

def preprocess_row(raw_row: Dict) -> Optional[Dict]:
    try:
        return {
            "epc":       str(raw_row["epc"]).strip(),
            "device":    str(raw_row["baselogicaldevice"]).strip(),
            "direction": str(raw_row["direction"]).strip(),
            "door":      str(raw_row["door"]).strip(),
            "rssi":      float(raw_row["rssi"]),  # float not int — sheets 4-7 have -75.5
            "t0":        int(raw_row["T0"]),
        }
    except (ValueError, KeyError):
        return None