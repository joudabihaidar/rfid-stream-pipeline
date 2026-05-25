import csv
import time
from typing import Generator, Dict


def stream_rfid_data(file_path: str) -> Generator[Dict[str, str], None, None]:
    """
    Simulates a live network stream by reading a CSV file row-by-row.
    Guarantees O(1) memory consumption by streaming lazily.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        # DictReader automatically maps the header row to dictionary keys
        reader = csv.DictReader(file)
        
        for row in reader:
            # Yield passes the single row to the pipeline without holding it in RAM
            yield row