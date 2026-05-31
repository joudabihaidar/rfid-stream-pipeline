# Real-Time People Tracking — RFID Pipeline

A real-time system that processes raw RFID data to detect and analyze
people's entry and exit events from a building.

---

**Main libraries:**

- `openpyxl` — reads raw RFID data directly from Excel 
- `sqlite3` — built-in Python library used for all database reads and writes
- `pandas` — data manipulation in the ML notebook and dashboard data loading
- `streamlit` — dashboard framework, auto-refresh, and interactive widgets
- `plotly` — bar charts, histograms, and data quality visualizations in the dashboard
- `scikit-learn` — StandardScaler for feature normalization, IsolationForest for anomaly scoring
- `matplotlib` + `seaborn` — visualizations in the ML exploration notebook

## How to run

### 1. Clone the repository

```bash
git clone <repo-url>
cd rfid-pipeline
```

### 2. Install dependencies

```bash
# Option A — UV (recommended)
uv sync

# Option B — pip
pip install -r requirements.txt
```

Requires Python 3.12+.

### 3. Run the pipeline

**Streaming mode** — processes data row by row at 20ms per row,
simulating a live RFID stream. Recommended for the demo.

```bash
# Terminal 1 — start the pipeline (~31 seconds for all 7 sessions)
python -m src.main --stream

# Terminal 2 — start the dashboard
streamlit run dashboard/app.py
```

Open `http://localhost:8501` in your browser.
Enable the **🔄 Auto-refresh** toggle in the sidebar to see events
appear in real time as the algorithm detects them.

**Batch mode** — processes everything at full speed (~2 seconds).

```bash
python -m src.main
streamlit run dashboard/app.py
```

> **Note:** To reset and reprocess from scratch, delete the database
> first: `del data\rfid_events.db` (Windows) or `rm data/rfid_events.db` (Mac/Linux).

---

## Output files

After the first run, two files are created in the `data/` directory:

| File | Description |
|---|---|
| `data/rfid_events.db` | SQLite database — all raw reads, events, anomalies, and sessions |
| `data/pipeline.log` | Full pipeline log with timestamps — INFO level on terminal, DEBUG level in file |

---

## Viewing the database

Download **DB Browser for SQLite** (free):
👉 https://sqlitebrowser.org/dl/

1. Open DB Browser
2. Click **Open Database**
3. Navigate to `rfid-pipeline/data/` and select `rfid_events.db`
4. Click the **Browse Data** tab and select a table from the dropdown

Four tables are available: `raw_reads`, `sessions`, `events`, `anomalies`.