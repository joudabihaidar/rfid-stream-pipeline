import time
import openpyxl
from typing import Generator, Dict

def stream_rfid_excel(file_path, sheet_name, delay=0.0):
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    ws = wb[sheet_name]

    rows    = iter(ws.rows)
    headers = [str(cell.value).strip().lower() for cell in next(rows)]

    # Read all rows into memory, sort by TagTime, then stream
    all_rows = []
    for row in rows:
        values = [cell.value for cell in row]
        record = dict(zip(headers, values))
        all_rows.append(record)

    # Sort by TagTime ascending before deriving T0
    all_rows.sort(key=lambda r: int(float(str(r["tagtime"]))))

    session_start_ms = int(float(str(all_rows[0]["tagtime"])))

    for record in all_rows:
        tag_time_ms  = int(float(str(record["tagtime"])))
        record["T0"] = round((tag_time_ms - session_start_ms) / 1000)
        yield record
        if delay > 0:
            time.sleep(delay)

    wb.close()