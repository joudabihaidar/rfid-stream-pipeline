# src/main.py
import openpyxl
from pathlib import Path
from typing import Dict, List

from pipeline import run_pipeline

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXCEL_PATH = PROJECT_ROOT / "data" / "raw_data.xlsx"

def run_all_sheets(file_path: str) -> Dict[str, List[Dict]]:
    """
    Runs the pipeline on every sheet in the workbook.
    Returns a dict keyed by TransactionID — the natural session identifier.
    """
    wb = openpyxl.load_workbook(file_path, read_only=True)
    sheet_names = wb.sheetnames
    wb.close()

    all_results = {}

    for sheet_name in sheet_names:
        print(f"\n{'═'*60}")
        print(f"  Processing sheet: {sheet_name}")
        print(f"{'═'*60}")

        events = run_pipeline(file_path, sheet_name)

        if not events:
            print("  No events detected.")
            continue

        # Use TransactionID from first event as the session key
        # It's in the data — more reliable than sheet name
        transaction_id = sheet_name  # fallback
        all_results[transaction_id] = events

        for e in events:
            tag   = f"[{e['event']:<16}]"
            
            # handle SESSION_ENDED_INSIDE which has t0=None
            t0    = str(e['t0']) if e['t0'] is not None else 'N/A'
            
            door  = e.get('door', '')
            zone  = f"{e.get('from_zone','').split('__')[-1]} → {e.get('to_zone','').split('__')[-1]}" if e['event'] == 'ZONE_TRANSITION' else ''
            dwell = f"  dwell={e['dwell_time']}s" if e.get('dwell_time') is not None else ''
            note  = f"  {e['note']}" if e.get('note') else ''
            
            print(f"  {tag}  t0={t0:<4}  {door or zone}{dwell}{note}")
    return all_results


if __name__ == "__main__":
    results = run_all_sheets(EXCEL_PATH)

    print(f"\n{'═'*60}")
    print(f"  SUMMARY")
    print(f"{'═'*60}")
    for session, events in results.items():
        entries = sum(1 for e in events if e['event'] == 'ENTRY')
        exits   = sum(1 for e in events if e['event'] == 'EXIT')
        print(f"  {session:<35}  entries={entries}  exits={exits}")