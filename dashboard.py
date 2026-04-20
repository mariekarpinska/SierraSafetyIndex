"""Sierra Safety Index — Streamlit dashboard.

All data from BigQuery dbt marts — never reads local files.
Three query functions at startup; everything else is display logic.

Data sources:
  mart_safety_score_history  → tile 2 (time-series)
  mart_crash_conditions      → tile 1 (conditions + crash context), tile 3 (crash map)
  raw_road_events            → tile 1 (current sensor readings)

⚠ Disclaimer: research tool only. Not driving advice. Check Caltrans QuickMap.
"""
from __future__ import annotations

import os
import warnings

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

warnings.filterwarnings("ignore", message="BigQuery Storage module not found")

# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _bq_client() -> bigquery.Client:
    """Thin wrapper — project from env so we don't hardcode it."""
    return bigquery.Client(project=os.getenv("GCP_PROJECT_ID"))


def _bq_query(sql: str) -> pd.DataFrame:
    """Run a BQ SQL query, return DataFrame. Raises on auth/schema errors."""
    client = _bq_client()
    return client.query(sql).to_dataframe()


DATASET = os.getenv("BIGQUERY_DATASET", "sierra_safety")
PROJECT = os.getenv("GCP_PROJECT_ID", "sierra-safety-index")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Sierra Safety Index",
    page_icon="🚞",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Corridor metadata — all 15 segments across 3 routes
# HWY-88 numbering: SEG_11-15 (note: not sequential with I-80/US-50)
# ---------------------------------------------------------------------------
SEG_META = [
    # I-80
    ("SEG_01", "Sacramento",      "I-80",   38.5816, -121.4944,   30),
    ("SEG_02", "Roseville",       "I-80",   38.7521, -121.2880,  164),
    ("SEG_03", "Auburn",          "I-80",   38.8966, -121.0769, 1255),
    ("SEG_04", "Colfax",          "I-80",   39.1002, -120.9533, 2421),
    ("SEG_05", "Emigrant_Gap",    "I-80",   39.2835, -120.6715, 5224),
    ("SEG_06", "Donner_Summit",   "I-80",   39.3232, -120.3253, 7227),
    ("SEG_07", "Truckee",         "I-80",   39.3280, -120.1833, 5817),
    # HWY-88 (Carson_Spur=SEG_13, Kirkwood=SEG_14, Carson_Pass=SEG_15)
    ("SEG_11", "Jackson",         "HWY-88", 38.3490, -120.7752, 1200),
    ("SEG_12", "Pioneer",         "HWY-88", 38.4330, -120.5707, 3300),
    ("SEG_13", "Carson_Spur",     "HWY-88", 38.7054, -120.1024, 7990),
    ("SEG_14", "Kirkwood",        "HWY-88", 38.6868, -120.0657, 7800),
    ("SEG_15", "Carson_Pass",     "HWY-88", 38.6940, -119.9800, 8573),
    # US-50
    ("SEG_08", "Placerville",     "US-50",  38.7296, -120.7985, 1867),
    ("SEG_09", "Echo_Summit",     "US-50",  38.8235, -120.0352, 7382),
    ("SEG_10", "South_Lake_Tahoe","US-50",  38.9399, -119.9772, 6237),
]
SEG_BY_ID    = {s[0]: s for s in SEG_META}
SEG_BY_NAME  = {s[1]: s for s in SEG_META}
ALL_SEG_NAMES = [s[1] for s in SEG_META]

BAND_ORDER  = ["GREEN", "YELLOW", "ORANGE", "RED", "BLACK"]
BAND_COLORS = {
    "GREEN":  "#2ca02c",
    "YELLOW": "#f0c040",
    "ORANGE": "#ff7f0e",
    "RED":    "#d62728",
    "BLACK":  "#1a1a1a",
}
BAND_BG = {
    "GREEN":  "background-color: #d5f5d5",
    "YELLOW": "background-color: #fdf6d3",
    "ORANGE": "background-color: #fde8cc",
    "RED":    "background-color: #fdd5d5",
    "BLACK":  "background-color: #ddd; color: #333",
}
ROUTE_COLORS = {"I-80": "#1f77b4", "HWY-88": "#ff7f0e", "US-50": "#2ca02c"}

def _fmt(s: str) -> str:
    """Display-safe segment name — removes underscores for human readers."""
    return str(s).replace("_", " ")

# ---------------------------------------------------------------------------
# Data loaders — ALL from BigQuery
# ---------------------------------------------------------------------------

# 60s TTL — matches the producer's 5-min interval, short enough for near-realtime feel
@st.cache_data(ttl=60)
def load_scores() -> pd.DataFrame:
    """Load hourly safety score history from dbt mart. Main dataset for tiles 1+2."""
    sql = f"""
        SELECT
            segment_id,
            segment_name,
            window_hour,
            avg_score,
            min_score,
            dominant_band,
            snowfall_events,
            chain_control_activations
        FROM `{PROJECT}.{DATASET}.mart_safety_score_history`
        ORDER BY window_hour DESC
        LIMIT 100000
    """
    df = _bq_query(sql)
    df["window_hour"] = pd.to_datetime(df["window_hour"], utc=True).dt.tz_localize(None)
    name_to_route = {s[1]: s[2] for s in SEG_META}
    df["route"] = df["segment_name"].map(name_to_route).fillna("I-80")
    df["score"] = df["avg_score"]
    df["band"]  = df["dominant_band"]
    df["event_timestamp"] = df["window_hour"]
    return df


@st.cache_data(ttl=300)
def load_risk_profiles() -> pd.DataFrame:
    """Load all-time risk profile per segment — changes slowly, 5min TTL is fine."""
    sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.mart_segment_risk_profile`
    """
    return _bq_query(sql)


@st.cache_data(ttl=300)
def load_current_conditions() -> pd.DataFrame:
    """Latest sensor reading per segment from the last 3 hrs.

    Queries raw_road_events directly (not a mart) since we want current values,
    not hourly aggregates. QUALIFY deduplicates to one row per segment.
    Returns empty DataFrame on any error — tile 1 gracefully degrades.
    """
    sql = f"""
        SELECT
            segment_id,
            segment_name,
            chain_control,
            snowfall_rate_in_hr,
            visibility_miles,
            wind_gust_mph,
            surface_temp_c,
            seismic_mag,
            score,
            band,
            event_timestamp
        FROM `{PROJECT}.{DATASET}.raw_road_events`
        WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_id ORDER BY event_timestamp DESC) = 1
    """
    try:
        df = _bq_query(sql)
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_crash_conditions(start_date, end_date) -> pd.DataFrame:
    """Load crash records for the selected date range from mart_crash_conditions."""
    sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.mart_crash_conditions`
        WHERE collision_datetime >= TIMESTAMP('{start_date}')
          AND collision_datetime <  TIMESTAMP(DATE_ADD('{end_date}', INTERVAL 1 DAY))
    """
    try:
        return _bq_query(sql)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Startup validation — fail loudly if BQ is unreachable or empty
# ---------------------------------------------------------------------------

try:
    scores_df = load_scores()
except Exception as exc:
    st.error(
        f"**BigQuery connection failed**: {exc}\n\n"
        "Ensure `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT_ID`, and `BIGQUERY_DATASET` "
        "are set correctly in your `.env` file and that dbt models have been materialized "
        "(`cd dbt_project && dbt run`)."
    )
    st.stop()

if scores_df.empty:
    st.warning(
        "**No data in BigQuery yet.**\n\n"
        "Run the pipeline to populate BigQuery:\n"
        "1. `make kafka-up` — start Kafka\n"
        "2. `python -m ingestion.producer` — produce events\n"
        "3. `python -m streaming.spark_consumer` — stream to BigQuery\n"
        "4. `python scripts/upload_to_bigquery.py` — upload backfill data\n"
        "5. `cd dbt_project && dbt run` — materialize dbt models"
    )
    st.stop()

# Load supporting data
try:
    risk_df = load_risk_profiles()
except Exception:
    risk_df = pd.DataFrame()

current_conditions_df = load_current_conditions()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
st.sidebar.title("Filters")
st.sidebar.caption("⚠️ Not driving advice — research tool only")

available_routes = sorted(scores_df["route"].dropna().unique().tolist())
selected_routes = st.sidebar.multiselect("Routes", options=available_routes, default=available_routes)

DEFAULT_SEGS = {
    "Donner_Summit", "Emigrant_Gap", "Truckee",
    "Pioneer", "Carson_Spur", "Kirkwood", "Carson_Pass",
}

available_segs = [
    s[1] for s in SEG_META
    if s[2] in selected_routes and s[1] in scores_df["segment_name"].unique()
]
default_segs = [s for s in available_segs if s in DEFAULT_SEGS]
selected_segs = st.sidebar.multiselect(
    "Segments (west → east / low → high)",
    options=available_segs,
    default=default_segs,
    format_func=_fmt,
)

min_ts = scores_df["event_timestamp"].min()
max_ts = scores_df["event_timestamp"].max()
default_start = max(min_ts.date(), (max_ts - pd.Timedelta(days=7)).date())
date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_ts.date()),
    min_value=min_ts.date(),
    max_value=max_ts.date(),
)
start_date, end_date = (
    (date_range[0], date_range[1]) if len(date_range) == 2
    else (default_start, max_ts.date())
)

crash_conditions_df = load_crash_conditions(start_date, end_date)
has_crashes = not crash_conditions_df.empty

mask = (
    scores_df["segment_name"].isin(selected_segs)
    & (scores_df["event_timestamp"].dt.date >= start_date)
    & (scores_df["event_timestamp"].dt.date <= end_date)
)
filtered = scores_df[mask].copy()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("Sierra Safety Index — I-80 · HWY-88 · HWY-89 · US-50 Dashboard")
st.caption(
    "Drive safety scores for Sierra Nevada mountain corridors, fused from "
    "Caltrans chain controls, RWIS sensors, NWS forecasts, Open-Meteo snowfall, "
    "USGS seismic data, and CCRS crash records. **Higher = safer.** "
    "⚠️ **This is a research tool, not driving advice.**"
)
st.divider()

if filtered.empty:
    st.warning("No data for current filter selection. Adjust filters or run the pipeline.")
    st.stop()

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total readings", f"{len(filtered):,}")
avg_s = filtered["score"].mean()
c2.metric("Avg score", f"{avg_s:.1f} / 100")
worst = filtered.loc[filtered["score"].idxmin()]
c3.metric("Lowest score", f"{_fmt(worst['segment_name'])}: {worst['score']:.0f}", delta=worst["band"], delta_color="off")
pct_green = (filtered["band"] == "GREEN").sum() / len(filtered) * 100
c4.metric("% GREEN", f"{pct_green:.1f}%")
c5.metric("Crash records", f"{len(crash_conditions_df):,}" if has_crashes else "No crash data")
st.divider()

seg_order = [s for s in ALL_SEG_NAMES if s in selected_segs]

# ---------------------------------------------------------------------------
# Tile 1 — Current conditions & historical crash context
# ---------------------------------------------------------------------------
st.subheader("Tile 1 · Current Conditions & Historical Crash Context")
st.caption(
    "**Current or recent conditions per segment** (last 3 hours from BigQuery) combined with "
    "historical crash counts sourced from the California Highway Patrol's "
    "[CCRS/SWITRS database](https://data.ca.gov/dataset/ccrs) (2024–2025). "
    "**Crashes are filtered to locations along I-80, US-50, HWY-88, and HWY-89** — only collisions "
    "within the corridor polygons for these highways are included. "
    "**Crashes (historical)** = total reported collisions near each segment over the selected date range — "
    "it is a measure of how crash-prone a location has been in the past, not a live incident count.\n\n"
    "Use this alongside the safety score to understand which segments have historically seen "
    "the most collisions so you can calibrate your risk tolerance. A segment with many past crashes "
    "and a low score today warrants extra caution.\n\n"
    "⚠️ **Disclaimer**: Correlation, not causation. Past crashes do not predict future crashes. "
    "This is NOT driving advice — always check [Caltrans QuickMap](https://quickmap.dot.ca.gov/) "
    "and use your own judgment."
)

if not current_conditions_df.empty:
    name_to_route = {s[1]: s[2] for s in SEG_META}
    cond = current_conditions_df[
        current_conditions_df["segment_name"].isin(selected_segs)
    ].copy()
    cond["route"] = cond["segment_name"].map(name_to_route).fillna("?")

    def _chain_label(v):
        if pd.isna(v) or v in (None, "None", ""):
            return "None"
        return str(v)

    def _snow_label(v):
        if pd.isna(v):
            return "—"
        v = float(v)
        if v >= 1.0:   return f"Heavy ({v:.1f} in/hr)"
        if v >= 0.5:   return f"Moderate ({v:.1f} in/hr)"
        if v >= 0.1:   return f"Light ({v:.1f} in/hr)"
        return "None"

    def _vis_label(v):
        if pd.isna(v):
            return "—"
        v = float(v)
        if v < 0.25:  return f"Very low ({v:.2f} mi)"
        if v < 0.5:   return f"Low ({v:.1f} mi)"
        if v < 1.0:   return f"Reduced ({v:.1f} mi)"
        return f"Good ({v:.1f} mi)"

    cond["Chain Control"] = cond["chain_control"].apply(_chain_label)
    cond["Snowfall"]      = cond["snowfall_rate_in_hr"].apply(_snow_label)
    cond["Visibility"]    = cond["visibility_miles"].apply(_vis_label)
    cond["Wind Gusts"]    = cond["wind_gust_mph"].apply(
        lambda v: f"{v:.0f} mph" if pd.notna(v) else "—"
    )
    cond["Surface Temp"]  = cond["surface_temp_c"].apply(
        lambda v: f"{v:.1f} °C" if pd.notna(v) else "—"
    )
    cond["Seismic"]       = cond["seismic_mag"].apply(
        lambda v: f"M{v:.1f}" if pd.notna(v) and float(v) >= 3.0 else "None"
    )
    cond["Safety Score"]  = cond["score"].apply(
        lambda v: f"{v:.0f}" if pd.notna(v) else "—"
    )
    cond["Band"] = cond["band"].fillna("—")

    if has_crashes and "segment_name" in crash_conditions_df.columns:
        agg_kwargs = {}
        if "case_id" in crash_conditions_df.columns:
            agg_kwargs["Crashes (historical)"] = ("case_id", "count")
        if "severity" in crash_conditions_df.columns:
            agg_kwargs["Most Severe"] = ("severity", lambda x: x.value_counts().index[0] if len(x) > 0 else "—")
        if "primary_factor" in crash_conditions_df.columns:
            agg_kwargs["Top Cause"] = ("primary_factor", lambda x: x.value_counts().index[0] if len(x) > 0 else "—")

        if agg_kwargs:
            crash_summary = (
                crash_conditions_df.groupby("segment_name")
                .agg(**agg_kwargs)
                .reset_index()
            )
            cond = cond.merge(crash_summary, on="segment_name", how="left")
    else:
        cond["Crashes (historical)"] = "No data"

    display_cols = [
        "segment_name", "route", "Band", "Safety Score",
        "Chain Control", "Snowfall", "Visibility", "Wind Gusts",
        "Surface Temp", "Seismic", "Crashes (historical)",
    ]
    if "Most Severe" in cond.columns:
        display_cols += ["Most Severe"]
    if "Top Cause" in cond.columns:
        display_cols += ["Top Cause"]
    display_cols = [c for c in display_cols if c in cond.columns]

    cond1 = cond[display_cols].copy()
    cond1["segment_name"] = cond1["segment_name"].map(_fmt)
    cond1 = cond1.rename(columns={"segment_name": "Segment"})
    display_cols1 = ["Segment"] + [c for c in display_cols if c != "segment_name"]
    styled1 = cond1[display_cols1].style.map(lambda v: BAND_BG.get(v, ""), subset=["Band"])
    st.dataframe(styled1, width='stretch')

    if has_crashes:
        if "severity" in crash_conditions_df.columns and "segment_name" in crash_conditions_df.columns:
            sev_counts = (
                crash_conditions_df[crash_conditions_df["segment_name"].isin(selected_segs)]
                .groupby(["segment_name", "severity"])
                .size()
                .reset_index(name="count")
            )
            if not sev_counts.empty:
                sev_counts["segment_name"] = sev_counts["segment_name"].map(_fmt)
                fig1b = px.bar(
                    sev_counts,
                    x="segment_name",
                    y="count",
                    color="severity",
                    barmode="stack",
                    title="Historical Crash Severity by Segment",
                    height=380,
                    labels={"segment_name": "Segment", "count": "Crashes", "severity": "Severity"},
                )
                st.plotly_chart(fig1b, width='stretch')
else:
    st.info(
        "**No live conditions data in BigQuery yet.** "
        "Run the producer (`python -m ingestion.producer`) to populate current readings."
    )
st.divider()

# ---------------------------------------------------------------------------
# Tile 2 — Safety score over time (temporal)
# ---------------------------------------------------------------------------
st.subheader("Tile 2 · Safety Score Over Time")
st.caption(
    "Each point is one pipeline reading (5-minute live or hourly backfill). "
    "Dotted reference lines mark band boundaries."
)

_filtered_plot = filtered.sort_values("event_timestamp").copy()
_filtered_plot["Segment"] = _filtered_plot["segment_name"].map(_fmt)
_seg_order_disp = [_fmt(s) for s in seg_order]

fig2 = px.line(
    _filtered_plot,
    x="event_timestamp",
    y="score",
    color="Segment",
    line_group="Segment",
    category_orders={"Segment": _seg_order_disp},
    markers=len(_filtered_plot) < 500,
    labels={"event_timestamp": "Time (UTC)", "score": "Safety Score (0–100)", "Segment": "Segment"},
    title="Safety Score Over Time — Sierra Nevada Corridors",
    height=450,
)
for val, label, color in [
    (80, "GREEN (80)", "#2ca02c"),
    (60, "YELLOW (60)", "#d4a800"),
    (40, "ORANGE (40)", "#ff7f0e"),
    (20, "RED (20)", "#d62728"),
]:
    fig2.add_hline(y=val, line_dash="dot", line_color=color, line_width=1.2,
                   annotation_text=label, annotation_position="right",
                   annotation_font_size=10, annotation_font_color=color)
fig2.update_layout(
    yaxis=dict(range=[0, 105]),
    xaxis_title="Date / Time (UTC)",
    yaxis_title="Safety Score (0 = closed, 100 = ideal)",
    legend_title_text="Segment",
    legend=dict(x=1.18, xanchor="left", y=1, yanchor="top"),
    margin=dict(r=220),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(size=13),
    hovermode="x unified",
)
st.plotly_chart(fig2, width='stretch')
st.divider()

# ---------------------------------------------------------------------------
# Tile 3 — Crash hotspot map (from BigQuery)
# ---------------------------------------------------------------------------
st.subheader("Tile 3 · Crash Hotspot Map")
st.caption(
    "All crashes within the I-80, HWY-88, HWY-89, and US-50 corridor polygons for the selected date range, "
    "colored by highway. Marker size reflects crash severity. "
    "⚠️ Research tool only — not driving advice."
)

if not has_crashes:
    st.info(
        "**No crash data in BigQuery yet.**\n\n"
        "To load crash data:\n"
        "1. `python -m ingestion.sources.ccrs_client --years 2024 2025 2026` — download CCRS data\n"
        "2. `python scripts/upload_to_bigquery.py --crashes` — load crash CSV into BigQuery\n"
        "3. `cd dbt_project && dbt run` — build the crash models in BigQuery\n"
        "4. `cd ..` and re-run `streamlit run dashboard.py` to refresh this view\n\n"
        "The map and crash stats panels will populate automatically on next refresh."
    )
else:
    seg_ref = pd.DataFrame([
        {"name": s[1], "route": s[2], "lat": s[3], "lon": s[4], "elev": s[5]}
        for s in SEG_META
    ])

    if "lat" in crash_conditions_df.columns and "lon" in crash_conditions_df.columns:
        # Tile 3 shows ALL corridor crashes (I-80, HWY-88, HWY-89, US-50),
        # not limited to selected segments — gives full hotspot picture.
        map_df = crash_conditions_df.dropna(subset=["lat", "lon"]).copy()
        if "hwy" in map_df.columns:
            map_df = map_df[map_df["hwy"].isin(["I-80", "HWY-88", "HWY-89", "US-50"])]
        sev_size = {
            "Fatal": 20, "Severe Injury": 14, "Other Visible Injury": 8,
            "Complaint of Pain": 6, "Property Damage Only": 4, "Unknown": 4,
        }
        map_df["marker_size"] = map_df.get("severity", pd.Series(dtype=str)).map(sev_size).fillna(4)

        HWY_COLORS = {"I-80": "#1f77b4", "HWY-88": "#ff7f0e", "HWY-89": "#9467bd", "US-50": "#2ca02c"}

        color_col = "hwy" if "hwy" in map_df.columns else (
            "primary_factor" if "primary_factor" in map_df.columns else None
        )
        hover_cols = {c: True for c in ["severity", "hwy", "collision_type", "primary_factor"]
                      if c in map_df.columns}

        fig3 = px.scatter_map(
            map_df,
            lat="lat",
            lon="lon",
            color=color_col,
            color_discrete_map=HWY_COLORS if color_col == "hwy" else None,
            size="marker_size",
            size_max=20,
            hover_data=hover_cols,
            title="CCRS Crash Locations — I-80 · HWY-88 · HWY-89 · US-50",
            map_style="open-street-map",
            zoom=7,
            center={"lat": 38.9, "lon": -120.5},
            height=550,
            opacity=0.7,
        )
        fig3.add_scattermap(
            lat=seg_ref["lat"],
            lon=seg_ref["lon"],
            mode="markers+text",
            marker=dict(size=10, color="navy", symbol="circle"),
            text=seg_ref["name"].map(_fmt),
            textposition="top right",
            textfont=dict(size=10, color="navy"),
            name="Monitoring Segments",
            hovertext=seg_ref.apply(
                lambda r: f"{_fmt(r['name'])} ({r['route']}, {r['elev']:,} ft)", axis=1
            ),
            hoverinfo="text",
        )
        legend_title = "Highway" if color_col == "hwy" else "Primary Cause"
        fig3.update_layout(legend_title_text=legend_title, font=dict(size=13))
        st.plotly_chart(fig3, width='stretch')

st.divider()

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.caption(
    "Sources: Caltrans CWWP2 (chain control, RWIS, CMS) · NWS hourly forecast · "
    "Open-Meteo snowfall · USGS seismic · CCRS crash records. "
    "Data flows: Kafka → Spark → GCS → BigQuery → dbt → Streamlit. "
    "⚠️ NOT driving advice."
)
