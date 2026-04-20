"""Upload local parquet data to GCS and BigQuery.

This is the primary way to get existing backfill / local-mode data into BigQuery.
Runs the full flow: local parquet → GCS upload → BigQuery load job.

Usage:
    python scripts/upload_to_bigquery.py               # upload road events
    python scripts/upload_to_bigquery.py --crashes     # also upload crash CSV
    python scripts/upload_to_bigquery.py --dry-run     # preview without uploading
    python scripts/upload_to_bigquery.py --no-gcs      # load directly (skip GCS step)

Prerequisites:
    1. python scripts/migrate_segments.py              # fix segment IDs first
    2. Set env vars: GCP_PROJECT_ID, GCS_PROCESSED_BUCKET, GOOGLE_APPLICATION_CREDENTIALS
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure the project root is on sys.path when running as scripts/upload_to_bigquery.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import structlog
from dotenv import load_dotenv

from ingestion.sources.corridor import classify_hwy, is_near_corridor, bq_corridor_filter_sql

load_dotenv()
log = structlog.get_logger()

# Columns present in BigQuery raw_road_events table (scalar types only)
BQ_SCALAR_COLS = [
    "segment_id", "segment_name", "route", "lat", "lon", "elevation_ft",
    "event_timestamp", "score", "band",
    "chain_control", "road_closed", "snowfall_rate_in_hr",
    "visibility_miles", "wind_gust_mph", "surface_temp_c", "seismic_mag",
]

CRASHES_CSV   = Path("data/switrs/switrs_crashes.csv")
PARQUET_DIR   = Path("data/processed/raw_events")


# ---------------------------------------------------------------------------
# Road events upload
# ---------------------------------------------------------------------------

def _load_local_parquets() -> pd.DataFrame:
    files = glob.glob(str(PARQUET_DIR / "*.parquet"))
    if not files:
        log.error("no_parquet_files_found", path=str(PARQUET_DIR))
        return pd.DataFrame()

    frames = []
    for f in files:
        try:
            part = pd.read_parquet(f, engine="pyarrow")
            frames.append(part)
        except Exception as exc:
            log.warning("parquet_read_failed", file=f, error=str(exc))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Ensure event_timestamp is tz-aware UTC
    if "event_timestamp" in df.columns:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
    elif "timestamp_utc" in df.columns:
        df["event_timestamp"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")

    df = df.dropna(subset=["event_timestamp", "segment_id"])

    # Add route column if missing (derive from segment_id prefix / name)
    if "route" not in df.columns or df["route"].isna().any():
        seg_route_map = {
            "SEG_01": "I-80",   "SEG_02": "I-80",  "SEG_03": "I-80",
            "SEG_04": "I-80",   "SEG_05": "I-80",  "SEG_06": "I-80",  "SEG_07": "I-80",
            "SEG_08": "US-50",  "SEG_09": "US-50", "SEG_10": "US-50",
            "SEG_11": "HWY-88", "SEG_12": "HWY-88","SEG_13": "HWY-88",
            "SEG_14": "HWY-88", "SEG_15": "HWY-88",
        }
        if "route" not in df.columns:
            df["route"] = df["segment_id"].map(seg_route_map).fillna("I-80")
        else:
            df["route"] = df["route"].fillna(df["segment_id"].map(seg_route_map)).fillna("I-80")

    # Keep only BQ-compatible scalar columns
    available = [c for c in BQ_SCALAR_COLS if c in df.columns]
    return df[available].copy()


def _upload_events_via_gcs(
    df: pd.DataFrame, gcs_bucket: str, project_id: str, dataset: str, dry_run: bool
) -> None:
    from google.cloud import bigquery, storage

    date_str  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    gcs_key   = f"raw_events/upload_{date_str}.parquet"
    temp_file = Path(f"data/tmp/upload_{date_str}.parquet")
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    import pyarrow as pa
    import pyarrow.parquet as pq

    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
    table = pa.Table.from_pandas(df, preserve_index=False)
    # Force event_timestamp to timestamp[us, UTC] — BigQuery requires this exact type
    if "event_timestamp" in table.schema.names:
        col_idx = table.schema.get_field_index("event_timestamp")
        table = table.set_column(
            col_idx, "event_timestamp",
            table.column("event_timestamp").cast(pa.timestamp("us", tz="UTC"))
        )
    pq.write_table(table, temp_file)
    log.info("parquet_written", rows=len(df), file=str(temp_file))

    if dry_run:
        print(f"[DRY RUN] Would upload {len(df):,} rows to gs://{gcs_bucket}/{gcs_key}")
        print(f"[DRY RUN] Would load into {project_id}.{dataset}.raw_road_events")
        temp_file.unlink(missing_ok=True)
        return

    # Upload to GCS
    sc = storage.Client()
    blob = sc.bucket(gcs_bucket).blob(gcs_key)
    blob.upload_from_filename(str(temp_file))
    uri = f"gs://{gcs_bucket}/{gcs_key}"
    log.info("uploaded_to_gcs", uri=uri)

    # Load into BigQuery
    bq = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.raw_road_events"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="event_timestamp"
        ),
        clustering_fields=["segment_id"],
    )
    job = bq.load_table_from_uri(uri, table_ref, job_config=job_config)
    job.result()
    log.info("loaded_into_bigquery", table=table_ref, rows=len(df))
    print(f"Loaded {len(df):,} road event rows into {table_ref}")

    temp_file.unlink(missing_ok=True)


def _upload_events_direct(
    df: pd.DataFrame, project_id: str, dataset: str, dry_run: bool
) -> None:
    """Load DataFrame directly into BigQuery without GCS (uses load_table_from_dataframe)."""
    from google.cloud import bigquery

    if dry_run:
        print(f"[DRY RUN] Would load {len(df):,} rows directly into {project_id}.{dataset}.raw_road_events")
        return

    bq = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.raw_road_events"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="event_timestamp"
        ),
        clustering_fields=["segment_id"],
    )
    job = bq.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    log.info("loaded_into_bigquery_direct", table=table_ref, rows=len(df))
    print(f"Loaded {len(df):,} road event rows into {table_ref} (direct load)")


# ---------------------------------------------------------------------------
# Crash data upload
# ---------------------------------------------------------------------------

_PRIMARY_FACTOR_RULES: list[tuple[str, str]] = [
    (r"2235[0O]|UNSAFE SPEED",                          "Unsafe Speed"),
    (r"22107|UNSAFE TURN",                              "Unsafe Turn / No Signal"),
    (r"21658|UNSAFE LANE CHANGE|LANED ROADWAY",         "Unsafe Lane Change"),
    (r"23152|23153|DRIVING UNDER INFLUENCE|UNDER THE INFLUENCE", "DUI"),
    (r"21453|STEADY CIRCULAR RED|RED LIGHT|RED ARROW",  "Red Light Violation"),
    (r"21804|ENTERING OR CROSSING",                     "Failure to Yield (Entering Hwy)"),
    (r"21802|FAIL TO STOP AT STOP|STOP SIGN",           "Failure to Yield (Stop Sign)"),
    (r"21801",                                          "Failure to Yield (Left Turn)"),
    (r"21800|21803|FAIL.*YIELD|FAILURE.*YIELD",         "Failure to Yield (Intersection)"),
    (r"21703|FOLLOWING TOO CLOS",                       "Following Too Closely"),
    (r"22106|UNSAFE START",                             "Unsafe Start from Stopped"),
    (r"2175[0-9]|UNSAFE PASS",                         "Unsafe Passing"),
    (r"21650|21651|WRONG SIDE|WRONG WAY",               "Wrong Side of Road"),
    (r"22450|STOP SIGN|FAIL TO STOP AT SIGN|FAILING TO STOP", "Stop Sign Violation"),
    (r"UNSAFE BACK|BACKING",                            "Unsafe Backing"),
]
_PRIMARY_FACTOR_COMPILED = [(re.compile(pat), label) for pat, label in _PRIMARY_FACTOR_RULES]


def _normalize_primary_factor_text(raw: str) -> str:
    """Map raw CCRS VC-code text to a clean dashboard-friendly label."""
    if not isinstance(raw, str):
        return "Unknown"
    upper = raw.upper().strip()
    if upper in ("", "UNKNOWN", "OTHER"):
        return "Unknown"
    for pattern, label in _PRIMARY_FACTOR_COMPILED:
        if pattern.search(upper):
            return label
    return "Other"


def _load_crash_csv() -> pd.DataFrame:
    if not CRASHES_CSV.exists():
        log.error("crashes_csv_not_found", path=str(CRASHES_CSV),
                  hint="Run: python -m ingestion.sources.ccrs_client --years 2024 2025 2026")
        return pd.DataFrame()

    df = pd.read_csv(CRASHES_CSV, dtype=str, low_memory=False)
    # Normalise: uppercase, strip whitespace, replace spaces with underscores
    df.columns = df.columns.str.upper().str.strip().str.replace(" ", "_", regex=False)

    sev_map = {
        "1": "Fatal", "2": "Severe Injury", "3": "Other Visible Injury",
        "4": "Complaint of Pain", "0": "Property Damage Only",
    }
    type_map = {
        "A": "Head-On", "B": "Sideswipe", "C": "Rear End", "D": "Broadside",
        "E": "Hit Object", "F": "Overturned", "G": "Vehicle/Pedestrian", "H": "Other",
    }
    pcf_map = {
        "01": "DUI", "02": "Improper Driving", "03": "Following Too Close",
        "04": "Wrong Side", "05": "Improper Passing", "06": "Unsafe Lane Change",
        "07": "Unsafe Speed", "08": "Traffic Signals/Signs", "09": "Unknown",
        "21": "Other Hazardous", "22": "Other Than Driver",
    }

    # Date column — old SWITRS: COLLISION_DATE, new CCRS 2026: CRASH_DATE_TIME
    date_col = next((c for c in ["COLLISION_DATE", "CRASH_DATE_TIME"] if c in df.columns), None)
    if not date_col:
        log.error("no_collision_date_column", columns=list(df.columns))
        return pd.DataFrame()

    # Case ID — old: CASE_ID, new CCRS 2026: COLLISION_ID
    case_id_col = next((c for c in ["CASE_ID", "COLLISION_ID", "OBJECTID"] if c in df.columns), None)

    out = pd.DataFrame()
    out["case_id"]            = df[case_id_col] if case_id_col else pd.Series(range(len(df)), dtype=str)
    out["collision_datetime"] = pd.to_datetime(df[date_col], errors="coerce", format="mixed").dt.tz_localize("UTC")
    out["lat"]                = pd.to_numeric(df.get("LATITUDE",  pd.Series(dtype=float)), errors="coerce")
    out["lon"]                = pd.to_numeric(df.get("LONGITUDE", pd.Series(dtype=float)), errors="coerce")

    # Severity — old: COLLISION_SEVERITY code, new CCRS 2026: derive from NUMBERKILLED/NUMBERINJURED
    if "COLLISION_SEVERITY" in df.columns:
        out["severity"] = df["COLLISION_SEVERITY"].map(sev_map).fillna("Unknown")
    else:
        killed  = pd.to_numeric(df.get("NUMBERKILLED",  pd.Series(["0"] * len(df))), errors="coerce").fillna(0)
        injured = pd.to_numeric(df.get("NUMBERINJURED", pd.Series(["0"] * len(df))), errors="coerce").fillna(0)
        out["severity"] = "Property Damage Only"
        out.loc[injured > 0, "severity"] = "Other Visible Injury"
        out.loc[killed  > 0, "severity"] = "Fatal"

    # Collision type
    type_col = next((c for c in ["TYPE_OF_COLLISION", "COLLISION_TYPE_CODE"] if c in df.columns), None)
    out["collision_type"] = df[type_col].map(type_map).fillna("Unknown") if type_col else "Unknown"

    # Primary factor — old: PCF_VIOL_CATEGORY code, new CCRS 2026: PRIMARY_COLLISION_FACTOR_VIOLATION (text)
    if "PCF_VIOL_CATEGORY" in df.columns:
        out["primary_factor"] = df["PCF_VIOL_CATEGORY"].map(pcf_map).fillna("Unknown")
    elif "PRIMARY_COLLISION_FACTOR_VIOLATION" in df.columns:
        out["primary_factor"] = df["PRIMARY_COLLISION_FACTOR_VIOLATION"].fillna("Unknown").apply(_normalize_primary_factor_text)
    elif "PRIMARY_COLLISION_FACTOR_CODE" in df.columns:
        out["primary_factor"] = df["PRIMARY_COLLISION_FACTOR_CODE"].map(pcf_map).fillna("Unknown")
    else:
        out["primary_factor"] = "Unknown"

    route_col = next((c for c in ["STATE_ROUTE", "ROUTE"] if c in df.columns), None)
    out["route"] = df[route_col] if route_col else "Unknown"

    out = out.dropna(subset=["collision_datetime", "lat", "lon"])
    out = out[(out["lat"] != 0) & (out["lon"] != 0)]
    # Classify each crash into its corridor (polygon-based; fallback to waypoint proximity).
    # Rows outside all 4 corridors (I-80, HWY-88, HWY-89, US-50) are dropped.
    out["hwy"] = [classify_hwy(lat, lon) for lat, lon in zip(out["lat"], out["lon"])]
    before = len(out)
    out = out[out["hwy"].notna()].reset_index(drop=True)
    log.info("crash_corridor_filter", kept=len(out), dropped=before - len(out))
    return out


def _upload_crashes(
    df: pd.DataFrame, project_id: str, dataset: str, gcs_bucket: str, dry_run: bool, use_gcs: bool
) -> None:
    from google.cloud import bigquery, storage

    if dry_run:
        print(f"[DRY RUN] Would upload {len(df):,} crash rows to {project_id}.{dataset}.crashes")
        return

    table_ref = f"{project_id}.{dataset}.crashes"
    schema = [
        bigquery.SchemaField("case_id",            "STRING"),
        bigquery.SchemaField("collision_datetime",  "TIMESTAMP"),
        bigquery.SchemaField("lat",                 "FLOAT64"),
        bigquery.SchemaField("lon",                 "FLOAT64"),
        bigquery.SchemaField("severity",            "STRING"),
        bigquery.SchemaField("collision_type",      "STRING"),
        bigquery.SchemaField("primary_factor",      "STRING"),
        bigquery.SchemaField("route",               "STRING"),
        bigquery.SchemaField("hwy",                 "STRING"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="collision_datetime"
        ),
        clustering_fields=["severity"],
    )

    bq = bigquery.Client(project=project_id)

    # BQ Parquet reader rejects nanosecond timestamps — downcast to microseconds.
    df = df.copy()
    if "collision_datetime" in df.columns:
        df["collision_datetime"] = df["collision_datetime"].dt.as_unit("us")

    if use_gcs and gcs_bucket:
        date_str  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        gcs_key   = f"crashes/upload_{date_str}.parquet"
        temp_file = Path(f"data/tmp/crashes_{date_str}.parquet")
        temp_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(temp_file, index=False, engine="pyarrow")
        sc = storage.Client()
        sc.bucket(gcs_bucket).blob(gcs_key).upload_from_filename(str(temp_file))
        uri = f"gs://{gcs_bucket}/{gcs_key}"
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job = bq.load_table_from_uri(uri, table_ref, job_config=job_config)
        temp_file.unlink(missing_ok=True)
    else:
        job = bq.load_table_from_dataframe(df, table_ref, job_config=job_config)

    job.result()
    log.info("crashes_loaded_to_bigquery", table=table_ref, rows=len(df))
    print(f"Loaded {len(df):,} crash rows into {table_ref}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _purge_off_corridor_crashes(project_id: str, dataset: str, dry_run: bool) -> None:
    """Delete crash rows in BigQuery that are not near any corridor waypoint."""
    filter_sql = bq_corridor_filter_sql()
    sql = f"DELETE FROM `{project_id}.{dataset}.crashes`\nWHERE NOT {filter_sql};"

    if dry_run:
        print("[DRY RUN] Would execute:\n" + sql)
        return

    from google.cloud import bigquery
    bq = bigquery.Client(project=project_id)
    job = bq.query(sql)
    job.result()
    log.info("bq_crash_purge_complete", table=f"{project_id}.{dataset}.crashes")
    print(f"Purged off-corridor crash rows from {project_id}.{dataset}.crashes")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local parquet data to BigQuery")
    parser.add_argument("--crashes",          action="store_true", help="Also upload crash CSV to BigQuery")
    parser.add_argument("--purge-bq-crashes", action="store_true", help="Delete off-corridor rows already in BigQuery crashes table")
    parser.add_argument("--dry-run",          action="store_true", help="Preview without writing")
    parser.add_argument("--no-gcs",           action="store_true", help="Load directly without GCS (slower for large datasets)")
    args = parser.parse_args()

    project_id  = os.getenv("GCP_PROJECT_ID")
    gcs_bucket  = os.getenv("GCS_PROCESSED_BUCKET", "")
    dataset     = os.getenv("BIGQUERY_DATASET", "sierra_safety")
    creds       = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    if not project_id:
        sys.exit("ERROR: GCP_PROJECT_ID not set in environment")
    if not creds or not Path(creds).exists():
        sys.exit(f"ERROR: GOOGLE_APPLICATION_CREDENTIALS not found: {creds}")

    print(f"Project: {project_id}  |  Dataset: {dataset}  |  GCS bucket: {gcs_bucket}")

    # --- Road events ---
    print("\nLoading local parquet files...")
    df = _load_local_parquets()
    if df.empty:
        print("No road event data found. Run the producer + consumer first, or run the backfill.")
    else:
        print(f"Found {len(df):,} road event rows across {df['segment_id'].nunique()} segments")
        print(f"  Date range: {df['event_timestamp'].min()} -> {df['event_timestamp'].max()}")
        if args.no_gcs or not gcs_bucket:
            _upload_events_direct(df, project_id, dataset, args.dry_run)
        else:
            _upload_events_via_gcs(df, gcs_bucket, project_id, dataset, args.dry_run)

    # --- Crash data ---
    if args.crashes:
        print("\nLoading crash CSV...")
        crash_df = _load_crash_csv()
        if crash_df.empty:
            print("No crash data found. Download first: python -m ingestion.sources.ccrs_client --years 2024 2025")
        else:
            print(f"Found {len(crash_df):,} crash rows")
            _upload_crashes(crash_df, project_id, dataset, gcs_bucket, args.dry_run, not args.no_gcs)

    # --- Purge off-corridor crashes already in BigQuery ---
    if args.purge_bq_crashes:
        print("\nPurging off-corridor crashes from BigQuery...")
        _purge_off_corridor_crashes(project_id, dataset, args.dry_run)


if __name__ == "__main__":
    main()
