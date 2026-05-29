import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from src.database import (
    get_all_sessions,
    get_raw_rows,
    get_all_events,
    get_events_for_session,
    get_all_anomalies,
    get_dwell_times,
    get_door_usage,
    get_zone_transition_paths,
    get_anomaly_summary,
)
from src.pipeline import stream_pipeline_events_from_rows

LOG_PATH = PROJECT_ROOT / "data" / "pipeline.log"


# ═══════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="RFID Building Access Tracker",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═══════════════════════════════════════════════════════════════
# DATA LOADING — no cache so streaming data appears immediately
# ═══════════════════════════════════════════════════════════════

sessions_df   = pd.DataFrame(get_all_sessions())
all_events_df = pd.DataFrame(get_all_events())
anomalies_df  = pd.DataFrame(get_all_anomalies())
dwell_df      = pd.DataFrame(get_dwell_times())
door_df       = pd.DataFrame(get_door_usage())
zone_df       = pd.DataFrame(get_zone_transition_paths())
anomaly_sum   = pd.DataFrame(get_anomaly_summary())

pipeline_has_run = not sessions_df.empty


# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("🏢 RFID Tracker")
    st.caption("Real-time building access monitoring")
    st.divider()

    session_ids = sessions_df["session_id"].tolist() if pipeline_has_run else []
    selected_session = st.selectbox(
        "Session",
        options=session_ids,
        index=0 if session_ids else None,
        help="Select a session to view its events",
    )

    st.divider()

    auto_refresh = st.toggle(
        "🔄 Auto-refresh",
        value=False,
        help=(
            "Enable when the streaming pipeline is running.\n"
            "Dashboard refreshes every 3s to show new events as "
            "the algorithm detects them."
        ),
    )
    if auto_refresh:
        st.caption("⚡ Live — refreshing every 3s")
    else:
        st.caption("Enable when streaming pipeline is running.")

    st.divider()
    if pipeline_has_run:
        st.caption(
            f"{len(sessions_df)} sessions · "
            f"{len(all_events_df)} events · "
            f"{len(anomalies_df)} anomalies"
        )
    else:
        st.caption("No data yet. Run the pipeline.")


# ═══════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════

tab1, tab2, tab3 = st.tabs([
    "📡 Live Feed",
    "📊 Access Patterns",
    "⚠️ Anomalies",
])


# ═══════════════════════════════════════════════════════════════
# TAB 1 — LIVE FEED
# ═══════════════════════════════════════════════════════════════

with tab1:
    if not pipeline_has_run:
        st.info(
            "No data yet. Start the streaming pipeline in a terminal:\n\n"
            "```\npython -m src.main --stream\n```\n\n"
            "Then enable **🔄 Auto-refresh** in the sidebar."
        )

    elif auto_refresh:
        # ── Streaming mode: show all events from all sessions ─────
        # All sessions are treated as one continuous feed.
        # Events are sorted most recent first so new arrivals appear
        # at the top as the pipeline writes them.
        st.subheader("Live event feed")

        entries_total = int(sessions_df["total_entries"].sum())
        exits_total   = int(sessions_df["total_exits"].sum())
        inside_now    = max(0, entries_total - exits_total)
        anom_total    = len(anomalies_df)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Currently inside",  inside_now)
        c2.metric("Total entries",     entries_total)
        c3.metric("Total exits",       exits_total)
        c4.metric("Anomalies flagged", anom_total,
                  delta="review" if anom_total > 0 else None,
                  delta_color="inverse")

        st.markdown("**All events — most recent first**")

        if not all_events_df.empty:
            feed = all_events_df.copy()
            feed = feed.rename(columns={
                "event_type": "Event",
                "t0":         "T0 (s)",
                "door":       "Door",
                "from_zone":  "From",
                "to_zone":    "To",
                "peak_rssi":  "Peak RSSI",
                "dwell_time": "Dwell (s)",
            })
            for col in ["From", "To"]:
                if col in feed.columns:
                    feed[col] = feed[col].str.split("__").str[-1]

            cols = [
                "session_id", "Event", "T0 (s)", "Door",
                "From", "To", "Dwell (s)", "Peak RSSI",
            ]
            cols = [c for c in cols if c in feed.columns]

            if "id" in feed.columns:
                feed = feed.sort_values("id", ascending=False)

            st.dataframe(feed[cols], width="stretch", height=500)
        else:
            st.info("No events yet — stream is starting...")

    else:
        # ── Static mode: show selected session ───────────────────
        # When auto-refresh is off, the dashboard shows a snapshot
        # of whatever was last written to the database.
        # Use the session selector to browse individual sessions.
        if not selected_session:
            st.info("Select a session from the sidebar.")
        else:
            session_row = sessions_df[
                sessions_df["session_id"] == selected_session
            ].iloc[0]

            st.subheader(f"Session: {selected_session}")
            st.caption(
                f"Date: {session_row.get('session_date', 'N/A')}  ·  "
                f"EPC: ...{str(session_row.get('epc', ''))[-8:]}  ·  "
                f"Entry signal: {session_row.get('entry_rssi_strength', 'N/A')}  ·  "
                f"Ghost ratio: "
                f"{float(session_row.get('ghost_read_ratio') or 0):.1%}"
            )
            st.divider()

            session_anomalies = (
                anomalies_df[anomalies_df["session_id"] == selected_session]
                if not anomalies_df.empty else pd.DataFrame()
            )
            n_anomalies = len(session_anomalies)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Entries",          int(session_row.get("total_entries", 0)))
            c2.metric("Exits",            int(session_row.get("total_exits", 0)))
            c3.metric("Zone transitions", int(session_row.get("total_transitions", 0)))
            c4.metric("Anomalies",        n_anomalies,
                      delta="flagged" if n_anomalies > 0 else None,
                      delta_color="inverse")

            st.markdown("**Event log**")
            session_events = get_events_for_session(selected_session)

            if session_events:
                df = pd.DataFrame(session_events)
                df = df.rename(columns={
                    "event_type": "Event",
                    "t0":         "T0 (s)",
                    "door":       "Door",
                    "from_zone":  "From zone",
                    "to_zone":    "To zone",
                    "peak_rssi":  "Peak RSSI",
                    "dwell_time": "Dwell (s)",
                })
                for col in ["From zone", "To zone"]:
                    if col in df.columns:
                        df[col] = df[col].str.split("__").str[-1]
                cols = [
                    "Event", "T0 (s)", "Door",
                    "From zone", "To zone", "Dwell (s)", "Peak RSSI",
                ]
                cols = [c for c in cols if c in df.columns]
                st.dataframe(df[cols], width="stretch", height=400)
            else:
                st.info("No events for this session.")


# ═══════════════════════════════════════════════════════════════
# TAB 2 — ACCESS PATTERNS
# ═══════════════════════════════════════════════════════════════

with tab2:
    if not pipeline_has_run:
        st.info("No data yet. Run the streaming pipeline first.")
    else:
        total_entries     = int(sessions_df["total_entries"].sum())
        total_exits       = int(sessions_df["total_exits"].sum())
        total_transitions = int(sessions_df["total_transitions"].sum())
        avg_dwell = dwell_df["dwell_time"].mean() if not dwell_df.empty else 0
        max_dwell = dwell_df["dwell_time"].max()  if not dwell_df.empty else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total entries",    total_entries)
        c2.metric("Total exits",      total_exits)
        c3.metric("Zone transitions", total_transitions)
        c4.metric("Avg dwell time",   f"{avg_dwell:.0f}s")
        c5.metric("Max dwell time",   f"{max_dwell:.0f}s")

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Entries and exits per session")
            chart_df = sessions_df[
                ["session_id", "total_entries", "total_exits"]
            ].copy()
            chart_df["session_id"] = chart_df["session_id"].str[-14:]
            melted = chart_df.melt(
                id_vars="session_id",
                value_vars=["total_entries", "total_exits"],
                var_name="type", value_name="count",
            )
            melted["type"] = melted["type"].map({
                "total_entries": "Entries",
                "total_exits":   "Exits",
            })
            fig = px.bar(
                melted, x="session_id", y="count",
                color="type", barmode="group",
                color_discrete_map={"Entries": "#1d9e75", "Exits": "#D85A30"},
            )
            fig.update_layout(margin=dict(t=10, b=10), height=300,
                              legend=dict(orientation="h"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Door usage")
            if not door_df.empty:
                fig = px.bar(
                    door_df, x="door", y="count",
                    color="event_type", barmode="group",
                    color_discrete_map={"ENTRY": "#1d9e75", "EXIT": "#D85A30"},
                )
                fig.update_layout(margin=dict(t=10, b=10), height=300,
                                  legend=dict(orientation="h"))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No door usage data yet.")

        st.divider()
        col3, col4 = st.columns(2)

        with col3:
            st.subheader("Dwell time distribution")
            if not dwell_df.empty:
                fig = px.histogram(
                    dwell_df, x="dwell_time", nbins=8,
                    labels={"dwell_time": "Dwell time (seconds)"},
                    color_discrete_sequence=["#534AB7"],
                )
                fig.update_layout(margin=dict(t=10, b=10), height=300)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No dwell time data yet.")

        with col4:
            st.subheader("Zone transition paths")
            if not zone_df.empty:
                z = zone_df.copy()
                z["from_zone"] = z["from_zone"].str.split("__").str[-1]
                z["to_zone"]   = z["to_zone"].str.split("__").str[-1]
                z.columns      = ["From", "To", "Count"]
                st.dataframe(z, width="stretch", height=300)
            else:
                st.info("No zone transitions yet.")

        st.divider()
        st.subheader("Data quality — ghost read ratio per session")
        st.caption(
            "Proportion of reads that were Out reads while the person "
            "was confirmed inside. Higher = noisier signal."
        )
        if "ghost_read_ratio" in sessions_df.columns:
            g = sessions_df[[
                "session_id", "ghost_read_ratio", "entry_rssi_strength"
            ]].copy()
            g["session_id"]          = g["session_id"].str[-14:]
            g["ghost_read_ratio"]    = g["ghost_read_ratio"].fillna(0)
            g["entry_rssi_strength"] = g["entry_rssi_strength"].fillna("unknown")
            fig = px.bar(
                g, x="session_id", y="ghost_read_ratio",
                color="entry_rssi_strength",
                color_discrete_map={
                    "strong":   "#1d9e75",
                    "moderate": "#BA7517",
                    "weak":     "#D85A30",
                    "unknown":  "#888780",
                },
                labels={
                    "session_id":          "Session",
                    "ghost_read_ratio":    "Ghost read ratio",
                    "entry_rssi_strength": "Entry signal",
                },
            )
            fig.update_layout(margin=dict(t=10, b=10), height=280,
                              yaxis_tickformat=".0%",
                              legend=dict(orientation="h"))
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# TAB 3 — ANOMALIES
# ═══════════════════════════════════════════════════════════════

with tab3:
    if not pipeline_has_run:
        st.info("No data yet. Run the streaming pipeline first.")
    elif anomalies_df.empty:
        st.success("✅ No anomalies detected.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total anomalies",
                  len(anomalies_df))
        c2.metric("Sessions with anomalies",
                  anomalies_df["session_id"].nunique())
        c3.metric("Clean sessions",
                  len(sessions_df) - anomalies_df["session_id"].nunique())

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Anomalies by type")
            if not anomaly_sum.empty:
                fig = px.bar(
                    anomaly_sum, x="anomaly_type", y="count",
                    labels={"anomaly_type": "Anomaly type", "count": "Count"},
                    color_discrete_sequence=["#D85A30"],
                )
                fig.update_layout(margin=dict(t=10, b=10), height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Anomalies per session")
            per = (
                anomalies_df.groupby("session_id")["anomaly_type"]
                .count().reset_index()
                .rename(columns={"anomaly_type": "count"})
            )
            per["session_id"] = per["session_id"].str[-14:]
            fig = px.bar(
                per, x="session_id", y="count",
                labels={"session_id": "Session", "count": "Anomaly count"},
                color_discrete_sequence=["#993C1D"],
            )
            fig.update_layout(margin=dict(t=10, b=10), height=300)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()
        st.subheader("Session overview")
        ov = sessions_df[[
            "session_id", "session_date", "total_entries", "total_exits",
            "total_transitions", "ghost_read_ratio",
            "entry_rssi_strength", "has_anomaly",
        ]].copy()
        ov["has_anomaly"] = ov["has_anomaly"].map({1: "⚠️ Yes", 0: "✅ No"})
        ov["ghost_read_ratio"] = ov["ghost_read_ratio"].apply(
            lambda x: f"{float(x):.1%}" if x is not None else "N/A"
        )
        ov.columns = [
            "Session", "Date", "Entries", "Exits",
            "Transitions", "Ghost ratio", "Entry signal", "Anomaly",
        ]
        st.dataframe(ov, width="stretch", height=280)

        st.divider()
        st.subheader("Full anomaly log")
        log_df = anomalies_df[[
            "session_id", "epc", "anomaly_type", "t0", "value", "note"
        ]].copy()
        log_df["epc"]        = log_df["epc"].str[-8:]
        log_df["session_id"] = log_df["session_id"].str[-14:]
        log_df.columns       = ["Session", "EPC", "Type", "T0 (s)", "Value", "Note"]
        st.dataframe(log_df, width="stretch", height=400)


# ═══════════════════════════════════════════════════════════════
# AUTO-REFRESH — at the very bottom so all content renders first
# ═══════════════════════════════════════════════════════════════

if auto_refresh:
    time.sleep(3)
    st.rerun()