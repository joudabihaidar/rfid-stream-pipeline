import csv
import time
from typing import Generator, Dict
from preprocessing import derive_t0, normalize_columns, parse_tagtime

def stream_rfid_data(file_path: str, delay: float = 0.0):
    """
    Streams rows from a CSV file, normalizing column names
    and deriving T0 for sheets that don't have it.
    """
    # First pass — get session start time for T0 derivation
    with open(file_path, mode='r', encoding='utf-8') as f:
        first_row = normalize_columns(next(csv.DictReader(f)))
        session_start_ms = parse_tagtime(first_row["TagTime"])

    # Second pass — stream and normalize
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            row = normalize_columns(raw_row)

            # Derive T0 if missing (sheets 4-7)
            if "T0" not in row or row["T0"].strip() == "":
                tag_time_ms = parse_tagtime(row["TagTime"])
                row["T0"]   = str(derive_t0(tag_time_ms, session_start_ms))

            yield row
            if delay > 0:
                time.sleep(delay)