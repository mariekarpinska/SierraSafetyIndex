"""Historical backfill for Sierra Safety Index.

Fetches ~6 months of hourly weather + quake data, scores each segment,
and writes results to local parquet and/or BigQuery.

Cost breakdown (all free):
  - Open-Meteo Archive API  : no key, unlimited historical
  - USGS FDSN catalog API   : no key, full earthquake history
  - BigQuery batch load     : first 10 GB/month free (dataset is ~5 MB)

Usage:
    python -m ingestion.backfill                          # last 180 days, all segments
    python -m ingestion.backfill --days 90                # last 90 days
    python -m ingestion.backfill --start 2024-10-01 --end 2025-04-01
    python -m ingestion.backfill --with-crashes           # include CCRS crash penalties
    python -m ingestion.backfill --to-bq                  # load into BigQuery (requires GCP creds)

Temporal correctness for crash scoring:
  When --with-crashes is set, each hourly score only uses crashes that
  occurred in the 24 hours BEFORE that reading's timestamp.  Future crashes
  (relative to the reading) are never used, so historical scores reflect
  what a driver would have known at that moment.
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
import structlog

from dotenv import load_dotenv

load_dotenv()

from ingestion.sources.usgs_client import _haversine_km_vec
from scoring.safety_score import SegmentReading, compute_score

log = structlog.get_logger()

# Columns written to BigQuery (matches spark_consumer.BQ_COLUMNS)
BQ_COLUMNS = [
    "segment_id", "segment_name", "route", "lat", "lon", "elevation_ft",
    "event_timestamp", "score", "band",
    "chain_control", "road_closed", "snowfall_rate_in_hr",
    "visibility_miles", "wind_gust_mph", "surface_temp_c", "seismic_mag",
]

# ---------------------------------------------------------------------------
# All corridors (I-80 + HWY-88 + US-50)
# ---------------------------------------------------------------------------
ALL_SEGMENTS = [
    # I-80
    {"segment_id": "SEG_01", "segment_name": "Sacramento",    "route": "I-80",   "lat": 38.5816, "lon": -121.4944, "elevation_ft": 30},
    {"segment_id": "SEG_02", "segment_name": "Roseville",     "route": "I-80",   "lat": 38.7521, "lon": -121.2880, "elevation_ft": 164},
    {"segment_id": "SEG_03", "segment_name": "Auburn",        "route": "I-80",   "lat": 38.8966, "lon": -121.0769, "elevation_ft": 1255},
    {"segment_id": "SEG_04", "segment_name": "Colfax",        "route": "I-80",   "lat": 39.1002, "lon": -120.9533, "elevation_ft": 2421},
    {"segment_id": "SEG_05", "segment_name": "Emigrant_Gap",  "route": "I-80",   "lat": 39.2835, "lon": -120.6715, "elevation_ft": 5224},
    {"segment_id": "SEG_06", "segment_name": "Donner_Summit", "route": "I-80",   "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227},
    {"segment_id": "SEG_07", "segment_name": "Truckee",       "route": "I-80",   "lat": 39.3280, "lon": -120.1833, "elevation_ft": 5817},
    # HWY-88
    {"segment_id": "SEG_11", "segment_name": "Jackson",       "route": "HWY-88", "lat": 38.3490, "lon": -120.7752, "elevation_ft": 1200},
    {"segment_id": "SEG_12", "segment_name": "Pioneer",       "route": "HWY-88", "lat": 38.4330, "lon": -120.5707, "elevation_ft": 3300},
    {"segment_id": "SEG_13", "segment_name": "Carson_Spur",   "route": "HWY-88", "lat": 38.7054, "lon": -120.1024, "elevation_ft": 7990},
    {"segment_id": "SEG_14", "segment_name": "Kirkwood",      "route": "HWY-88", "lat": 38.6868, "lon": -120.0657, "elevation_ft": 7800},
    {"segment_id": "SEG_15", "segment_name": "Carson_Pass",   "route": "HWY-88", "lat": 38.6940, "lon": -119.9800, "elevation_ft": 8573},
    # US-50
    {"segment_id": "SEG_08", "segment_name": "Placerville",   "route": "US-50",  "lat": 38.7296, "lon": -120.7985, "elevation_ft": 1867},
    {"segment_id": "SEG_09", "segment_name": "Echo_Summit",   "route": "US-50",  "lat": 38.8235, "lon": -120.0352, "elevation_ft": 7382},
    {"segment_id": "SEG_10", "segment_name": "South_Lake_Tahoe","route":"US-50", "lat": 38.9399, "lon": -119.9772, "elevation_ft": 6237},
]

OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
USGS_CATALOG_URL      = "https://earthquake.usgs.gov/fdsnws/event/1/query"
CRASHES_CSV           = Path("data/switrs/switrs_crashes.csv")

OUTPUT_DIR = Path("data/processed/raw_events")


# ---------------------------------------------------------------------------
# Open-Meteo historical fetch
# ---------------------------------------------------------------------------

def fetch_historical_weather(
    lat: float, lon: float, start: date, end: date
) -> pd.DataFrame:
    """Fetch hourly weather archive for one lat/lon from Open-Meteo archive API.

    Returns a DataFrame with columns:
      time, snowfall_cm, wind_gusts_kmh, visibility_m, surface_temp_c

    Free, no key. Rate limit is generous — one call per segment is fine.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": "snowfall,wind_gusts_10m,visibility,surface_temperature",
        "wind_speed_unit": "kmh",
        "timezone": "UTC",
    }
    resp = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=30)
    resp.raise_for_status()
    hourly = resp.json().get("hourly", {})
    df = pd.DataFrame({
        "time":           pd.to_datetime(hourly.get("time", [])),
        "snowfall_cm":    hourly.get("snowfall", []),
        "wind_gusts_kmh": hourly.get("wind_gusts_10m", []),
        "visibility_m":   hourly.get("visibility", []),
        "surface_temp_c": hourly.get("surface_temperature", []),
    })
    return df


# ---------------------------------------------------------------------------
# USGS historical fetch
# ---------------------------------------------------------------------------

def fetch_historical_quakes(start: date, end: date) -> pd.DataFrame:
    """Fetch quake catalog from USGS FDSN for the Sierra Nevada bounding box.

    One call for the whole date range (much cheaper than per-segment calls).
    Caller then does per-segment proximity filtering via _nearest_quake_mag.
    minmagnitude=2.5 — anything smaller isn't a rockfall risk on these roads.
    """
    params = {
        "format":       "geojson",
        "starttime":    start.isoformat(),
        "endtime":      end.isoformat(),
        "minlatitude":  38.0,
        "maxlatitude":  40.5,
        "minlongitude": -122.0,
        "maxlongitude": -119.0,
        "minmagnitude": 2.5,
    }
    resp = requests.get(USGS_CATALOG_URL, params=params, timeout=60)
    resp.raise_for_status()
    features = resp.json().get("features", [])
    rows = []
    for f in features:
        props = f["properties"]
        coords = f["geometry"]["coordinates"]
        rows.append({
            "eq_time": datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc).replace(tzinfo=None),
            "eq_lat":  float(coords[1]),
            "eq_lon":  float(coords[0]),
            "eq_mag":  float(props["mag"]),
        })
    log.info("quakes_fetched", count=len(rows), start=start, end=end)
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["eq_time", "eq_lat", "eq_lon", "eq_mag"])


def _nearest_quake_mag(quakes_df: pd.DataFrame, ts: datetime, lat: float, lon: float) -> Optional[float]:
    """Highest M within 80 km and 6 hrs before ts, or None.

    6-hr window matches the live producer's USGS feed lookback.
    """
    if quakes_df.empty:
        return None
    window = quakes_df[
        (quakes_df["eq_time"] >= ts - timedelta(hours=6))
        & (quakes_df["eq_time"] <= ts)
    ]
    if window.empty:
        return None
    dists = _haversine_km_vec(lat, lon, window["eq_lat"].to_numpy(), window["eq_lon"].to_numpy())
    nearby = window[dists <= 80.0]
    return float(nearby["eq_mag"].max()) if not nearby.empty else None


# ---------------------------------------------------------------------------
# Crash data helpers
# ---------------------------------------------------------------------------

def _load_crashes_for_backfill() -> dict[date, pd.DataFrame]:
    """Load the CCRS crash CSV and bucket by collision date.

    Bucketing by date lets the per-hour scoring loop do O(1) dict lookups
    instead of scanning the full crash df for each of the ~26k hourly readings.
    Returns empty dict if CSV doesn't exist yet — crash scoring just skips.
    """
    if not CRASHES_CSV.exists():
        log.warning("crashes_csv_not_found", path=str(CRASHES_CSV),
                    hint="Run: python -m ingestion.sources.ccrs_client --daily")
        return {}

    df = pd.read_csv(CRASHES_CSV, dtype=str, low_memory=False)
    df.columns = df.columns.str.upper().str.strip()

    date_col = next((c for c in ("COLLISION_DATE",) if c in df.columns), None)
    if not date_col:
        return {}

    df["_crash_dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df["_lat"] = pd.to_numeric(df.get("LATITUDE",  pd.Series(dtype=float)), errors="coerce")
    df["_lon"] = pd.to_numeric(df.get("LONGITUDE", pd.Series(dtype=float)), errors="coerce")

    sev_map = {"1": "Fatal", "2": "Severe Injury", "3": "Other Visible Injury",
               "4": "Complaint of Pain", "0": "Property Damage Only"}
    if "COLLISION_SEVERITY" in df.columns:
        df["_severity"] = df["COLLISION_SEVERITY"].map(sev_map).fillna("Unknown")
    else:
        df["_severity"] = "Unknown"

    df = df.dropna(subset=["_crash_dt", "_lat", "_lon"])
    df = df[(df["_lat"] != 0) & (df["_lon"] != 0)]

    buckets: dict[date, pd.DataFrame] = {}
    for crash_date, group in df.groupby(df["_crash_dt"].dt.date):
        buckets[crash_date] = group.reset_index(drop=True)
    return buckets


def _crash_penalty_at(
    buckets: dict[date, pd.DataFrame],
    ts: datetime,
    lat: float,
    lon: float,
    radius_km: float = 16.0,
) -> tuple[int, int, str]:
    """Return (penalty, count, severity) for crashes in the 24 h before ts.

    Only accesses crash buckets for [ts-24h, ts), so no future data leaks
    into historical scores.
    """
    if not buckets:
        return 0, 0, "None"

    cutoff = ts - timedelta(hours=24)
    candidate_days = {cutoff.date(), ts.date()}

    parts = [buckets[d] for d in candidate_days if d in buckets]
    if not parts:
        return 0, 0, "None"

    nearby_df = pd.concat(parts, ignore_index=True)
    # Temporal filter: [cutoff, ts)
    nearby_df = nearby_df[
        (nearby_df["_crash_dt"] >= pd.Timestamp(cutoff))
        & (nearby_df["_crash_dt"] < pd.Timestamp(ts))
    ]
    if nearby_df.empty:
        return 0, 0, "None"

    dists = _haversine_km_vec(lat, lon, nearby_df["_lat"].to_numpy(), nearby_df["_lon"].to_numpy())
    nearby_df = nearby_df[dists <= radius_km]
    if nearby_df.empty:
        return 0, 0, "None"

    count = len(nearby_df)
    sev_order = ["Fatal", "Severe Injury", "Other Visible Injury",
                 "Complaint of Pain", "Property Damage Only", "Unknown"]
    max_sev = "Unknown"
    for sev in sev_order:
        if sev in nearby_df["_severity"].values:
            max_sev = sev
            break

    if max_sev == "Fatal":
        penalty = 20
    elif max_sev == "Severe Injury":
        penalty = 12
    elif max_sev in ("Other Visible Injury", "Complaint of Pain"):
        penalty = 6
    else:
        penalty = min(3 * count, 9)

    return min(penalty, 25), count, max_sev


# ---------------------------------------------------------------------------
# Safety scoring helpers
# ---------------------------------------------------------------------------
CM_TO_IN   = 0.393701
KMH_TO_MPH = 0.621371
M_TO_MILES = 0.000621371


def _infer_chain_control(snowfall_in_hr: float) -> Optional[str]:
    """Infer CC level from snowfall rate — no historical CC data available from Caltrans.

    These thresholds are rough approximations. Actual CC decisions involve
    road surface conditions, forecast, and Caltrans judgment calls.
    """
    if snowfall_in_hr >= 1.0:
        return "R3"
    if snowfall_in_hr >= 0.5:
        return "R2"
    if snowfall_in_hr >= 0.2:
        return "R1"
    return None


# ---------------------------------------------------------------------------
# BigQuery loader
# ---------------------------------------------------------------------------

def _widen_bq_schema(bq_client, table_ref: str, float_cols: list[str]) -> None:
    """Widen INTEGER columns to FLOAT64 via ALTER COLUMN DDL.

    Needed when a prior load inferred INTEGER from whole-number float values
    (e.g. visibility_miles=10.0 → pyarrow int64 → BQ INTEGER). BQ allows
    INTEGER → FLOAT64 widening but not the reverse.
    update_table() can't change types — DDL is the only way.
    """
    from google.api_core.exceptions import NotFound

    try:
        table = bq_client.get_table(table_ref)
    except NotFound:
        return  # table doesn't exist yet; BQ will create it on first load

    for field in table.schema:
        if field.name in float_cols and field.field_type == "INTEGER":
            ddl = (
                f"ALTER TABLE `{table_ref}` "
                f"ALTER COLUMN `{field.name}` SET DATA TYPE FLOAT64"
            )
            log.info("widening_bq_column", column=field.name, ddl=ddl)
            bq_client.query(ddl).result()
            log.info("bq_column_widened", column=field.name, table=table_ref)


def _load_to_bigquery(df: pd.DataFrame, chunk_size: int = 10_000) -> None:
    """Chunk the backfill DataFrame and load to BigQuery via GCS.

    Mirrors spark_consumer._write_to_gcs_and_bq so partitioning/clustering
    matches. Uses GCS staging to avoid BQ streaming insert quotas.
    Falls back to direct pandas → BQ when GCS_RAW_BUCKET not configured.
    """
    from google.cloud import bigquery, storage

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset    = os.environ.get("BIGQUERY_DATASET", "sierra_safety")
    gcs_bucket = os.environ.get("GCS_RAW_BUCKET", "")
    table_ref  = f"{project_id}.{dataset}.raw_road_events"

    available = [c for c in BQ_COLUMNS if c in df.columns]
    pdf_bq = df[available].copy()
    # Cast to microsecond precision: BigQuery rejects nanosecond timestamps from parquet
    pdf_bq["event_timestamp"] = (
        pd.to_datetime(pdf_bq["event_timestamp"], utc=True)
        .astype("datetime64[us, UTC]")
    )
    pdf_bq = pdf_bq.dropna(subset=["event_timestamp"])

    # Force float columns to float64 so pyarrow never infers INTEGER from
    # whole-number values (e.g. visibility_miles=10.0 → int64 → BQ type mismatch)
    float_cols = ["lat", "lon", "snowfall_rate_in_hr", "visibility_miles",
                  "wind_gust_mph", "surface_temp_c", "seismic_mag"]
    for col in float_cols:
        if col in pdf_bq.columns:
            pdf_bq[col] = pdf_bq[col].astype("float64")
    if "score" in pdf_bq.columns:
        pdf_bq["score"] = pdf_bq["score"].astype("Int64")
    if "elevation_ft" in pdf_bq.columns:
        pdf_bq["elevation_ft"] = pdf_bq["elevation_ft"].astype("Int64")

    bq_client = bigquery.Client(project=project_id)

    # Widen any INTEGER columns in the existing table that should be FLOAT.
    # BQ allows INTEGER → FLOAT relaxation; the reverse is rejected.
    _widen_bq_schema(bq_client, table_ref, float_cols)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_timestamp",
        ),
        clustering_fields=["segment_id"],
    )

    if gcs_bucket:
        # Upload parquet to GCS, then load via URI (avoids streaming quota limits)
        storage_client = storage.Client()
        bucket_obj     = storage_client.bucket(gcs_bucket)
        run_tag        = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        for i in range(0, len(pdf_bq), chunk_size):
            chunk = pdf_bq.iloc[i : i + chunk_size]
            temp_file = Path(f"./data/tmp/backfill_chunk_{i}.parquet")
            temp_file.parent.mkdir(parents=True, exist_ok=True)
            chunk.to_parquet(temp_file, index=False, engine="pyarrow",
                             coerce_timestamps="us", allow_truncated_timestamps=True)

            gcs_key = f"backfill/{run_tag}/chunk_{i}.parquet"
            blob = bucket_obj.blob(gcs_key)
            blob.upload_from_filename(str(temp_file))
            temp_file.unlink(missing_ok=True)

            uri = f"gs://{gcs_bucket}/{gcs_key}"
            job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
            job.result()
            log.info("chunk_loaded_to_bq", chunk_start=i, rows=len(chunk), table=table_ref)
    else:
        # Direct pandas → BQ (no GCS bucket configured)
        for i in range(0, len(pdf_bq), chunk_size):
            chunk = pdf_bq.iloc[i : i + chunk_size]
            job = bq_client.load_table_from_dataframe(chunk, table_ref, job_config=job_config)
            job.result()
            log.info("chunk_loaded_to_bq_direct", chunk_start=i, rows=len(chunk), table=table_ref)

    log.info("bigquery_load_complete", total_rows=len(pdf_bq), table=table_ref)
    print(f"BigQuery load complete: {len(pdf_bq):,} rows → {table_ref}")


# ---------------------------------------------------------------------------
# Main backfill routine
# ---------------------------------------------------------------------------

def backfill(
    start: date,
    end: date,
    segments: list[dict] | None = None,
    with_crashes: bool = False,
    to_bq: bool = False,
) -> None:
    """Backfill historical scored readings for all (or specified) segments.

    Writes parquet to data/processed/raw_events/ and optionally loads the
    result into BigQuery (--to-bq).

    with_crashes: incorporate CCRS crash data into scoring; each hourly score
      only uses crashes in the 24 h BEFORE that timestamp.
    to_bq: upload results to BigQuery via GCS (requires GCP_PROJECT_ID env var
      and valid GOOGLE_APPLICATION_CREDENTIALS).
    """
    if segments is None:
        segments = ALL_SEGMENTS

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("backfill_start", start=start.isoformat(), end=end.isoformat(),
             segments=len(segments), with_crashes=with_crashes)

    # Optionally load crash data bucketed by date for efficient lookup
    crash_buckets: dict[date, pd.DataFrame] = {}
    if with_crashes:
        log.info("loading_crash_data")
        crash_buckets = _load_crashes_for_backfill()
        log.info("crash_buckets_loaded", dates=len(crash_buckets))

    # Fetch quakes once for the whole period (one API call)
    log.info("fetching_earthquake_catalog")
    quakes_df = fetch_historical_quakes(start, end)

    all_rows: list[dict] = []

    for seg in segments:
        seg_id   = seg["segment_id"]
        seg_name = seg["segment_name"]
        lat      = seg["lat"]
        lon      = seg["lon"]

        log.info("fetching_weather", segment=seg_name, start=start, end=end)
        try:
            wx = fetch_historical_weather(lat, lon, start, end)
        except Exception as exc:
            log.error("weather_fetch_failed", segment=seg_name, error=str(exc))
            continue

        for _, row in wx.iterrows():
            ts: datetime = row["time"]
            if pd.isna(ts):
                continue

            snowfall_cm   = float(row["snowfall_cm"])   if pd.notna(row["snowfall_cm"])   else 0.0
            gust_kmh      = float(row["wind_gusts_kmh"]) if pd.notna(row["wind_gusts_kmh"]) else None
            vis_m         = float(row["visibility_m"])   if pd.notna(row["visibility_m"])   else None
            surf_temp     = float(row["surface_temp_c"]) if pd.notna(row["surface_temp_c"]) else None

            snowfall_in_hr  = snowfall_cm * CM_TO_IN
            wind_gust_mph   = gust_kmh * KMH_TO_MPH if gust_kmh is not None else None
            visibility_miles= vis_m * M_TO_MILES    if vis_m  is not None else None
            chain_control   = _infer_chain_control(snowfall_in_hr)
            seismic_mag     = _nearest_quake_mag(quakes_df, ts, lat, lon)

            # Crash penalty: only crashes in the 24 h before this reading
            crash_pen, crash_count, crash_sev = (
                _crash_penalty_at(crash_buckets, ts, lat, lon)
                if crash_buckets else (0, 0, "None")
            )

            result = compute_score(SegmentReading(
                segment_id=seg_id,
                chain_control=chain_control,
                snowfall_rate_in_hr=snowfall_in_hr,
                visibility_miles=visibility_miles,
                wind_gust_mph=wind_gust_mph,
                surface_temp_c=surf_temp,
                seismic_mag=seismic_mag,
                recent_crash_count=crash_count if crash_count > 0 else None,
                recent_crash_severity=crash_sev if crash_sev != "None" else None,
            ))
            score, band = result.score, result.band

            all_rows.append({
                "segment_id":          seg_id,
                "segment_name":        seg_name,
                "route":               seg["route"],
                "lat":                 lat,
                "lon":                 lon,
                "elevation_ft":        seg["elevation_ft"],
                "timestamp_utc":       ts.isoformat(),
                "event_timestamp":     ts,
                "score":               score,
                "band":                band,
                "active_penalties":    [],
                "chain_control":       chain_control,
                "road_closed":         None,
                "snowfall_rate_in_hr": snowfall_in_hr,
                "visibility_miles":    visibility_miles,
                "wind_gust_mph":       wind_gust_mph,
                "surface_temp_c":      surf_temp,
                "seismic_mag":         seismic_mag,
            })

    if not all_rows:
        log.warning("no_rows_produced")
        return

    df = pd.DataFrame(all_rows)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    out_path = OUTPUT_DIR / f"backfill_{start}_{end}.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    log.info("backfill_complete", rows=len(df), output=str(out_path))
    print(f"\nBackfill complete: {len(df):,} rows -> {out_path}")
    print(f"Segments: {df['segment_name'].nunique()} | "
          f"Date range: {df['event_timestamp'].min()} to {df['event_timestamp'].max()}")
    print(df.groupby("segment_name")[["score"]].agg(["mean", "min"]).round(1).to_string())

    if to_bq:
        log.info("loading_to_bigquery", rows=len(df))
        _load_to_bigquery(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical safety scores")
    parser.add_argument("--days",  type=int,  default=180, help="Number of past days (default 180)")
    parser.add_argument("--start", type=str,  help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--end",   type=str,  help="End date YYYY-MM-DD (default today)")
    parser.add_argument("--with-crashes", action="store_true",
                        help="Include CCRS crash penalties (requires data/switrs/switrs_crashes.csv)")
    parser.add_argument("--to-bq", action="store_true",
                        help="Load results into BigQuery (requires GCP_PROJECT_ID + credentials)")
    args = parser.parse_args()

    today = date.today()
    end_date   = date.fromisoformat(args.end)   if args.end   else today
    start_date = date.fromisoformat(args.start) if args.start else today - timedelta(days=args.days)

    backfill(start_date, end_date, with_crashes=args.with_crashes, to_bq=args.to_bq)
