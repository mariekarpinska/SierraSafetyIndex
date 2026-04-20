"""CCRS (California Crash Reporting System) downloader.

Source: data.ca.gov — public CKAN API, no auth.
CCRS replaced SWITRS as the state crash DB around 2023. Column names changed;
CCRS_COL_MAP handles the mapping so downstream code stays consistent.

Downloads statewide yearly CSVs (each ~34 MB), filters to corridor routes,
writes to data/switrs/switrs_crashes.csv. Streaming chunks so we don't OOM
on the full state file.

Daily incremental mode (--daily): re-downloads current-year CSV, appends
only rows newer than the checkpoint. Avoids full rebuild every day.

Usage:
    python -m ingestion.sources.ccrs_client            # last 2 calendar years
    python -m ingestion.sources.ccrs_client --years 2024 2025
    python -m ingestion.sources.ccrs_client --daily    # incremental update
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd
import requests
import structlog

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# CKAN resource IDs on data.ca.gov  (update if the dataset is re-published)
# Verify at: https://data.ca.gov/dataset/ccrs
# ---------------------------------------------------------------------------
CKAN_BASE      = "https://data.ca.gov"
CKAN_API       = f"{CKAN_BASE}/api/3/action"

CCRS_RESOURCE_IDS: dict[int, str] = {
    2026: "b8ce0ca4-b4e9-490d-b4d1-1f4ec48cbefb",
    2025: "9f4fc839-122d-4595-a146-43bc4ed16f46",
    2024: "f775df59-b89b-4f82-bd3d-8807fa3a22a0",
}

from ingestion.sources.switrs_client import CORRIDOR_ROUTES
from ingestion.sources.corridor import is_near_corridor

OUTPUT_CSV = Path("data/switrs/switrs_crashes.csv")
CHUNK_SIZE = 1024 * 256   # 256 KB streaming chunks

# ---------------------------------------------------------------------------
# CCRS → internal schema mapping
# CCRS uses different column names than the older SWITRS export.
# We keep both so the file works whether the user has CCRS or SWITRS data.
# ---------------------------------------------------------------------------
CCRS_COL_MAP = {
    # CCRS name           : internal name  (identity mappings omitted)
    "CASE_ID":              "case_id",
    # CCRS may use these alternative column names:
    "PRIMARY_COLL_FACTOR":  "PCF_VIOLATION_CATEGORY",
    "COLL_SEVERITY":        "COLLISION_SEVERITY",
    "COLL_TYPE":            "TYPE_OF_COLLISION",
    "VEH_TYPE":             "VEHICLE_TYPE",
    "STATE_ROUTE":          "ROUTE",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_download_url(resource_id: str) -> str:
    """Resolve direct CSV URL from CKAN resource ID.

    data.ca.gov uses CKAN — resource_show gives us the actual download URL
    which can change when the dataset is re-published.
    """
    resp = requests.get(
        f"{CKAN_API}/resource_show",
        params={"id": resource_id},
        timeout=20,
    )
    resp.raise_for_status()
    result = resp.json().get("result", {})
    url = result.get("url", "")
    if not url:
        raise ValueError(f"No URL found for resource {resource_id}")
    log.info("ccrs_resource_resolved", resource_id=resource_id, url=url)
    return url


def _stream_and_filter(url: str, routes: set[str]) -> pd.DataFrame:
    """Stream the CCRS CSV in 50k-row chunks, keep only corridor routes + proximity.

    Two-stage filter: first by route number (fast), then by distance to corridor
    waypoints (catches crashes on connectors/side roads that share the route).
    """
    log.info("ccrs_download_start", url=url)
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    resp.raw.decode_content = True

    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(resp.raw, chunksize=50_000, dtype=str, low_memory=False):
        chunk.columns = chunk.columns.str.upper().str.strip()

        # Normalise column names
        chunk = chunk.rename(columns={k: v for k, v in CCRS_COL_MAP.items() if k in chunk.columns})

        # Detect route column
        route_col = next((c for c in ("ROUTE", "STATE_ROUTE") if c in chunk.columns), None)
        if route_col is None:
            frames.append(chunk)   # can't filter — keep all, let caller handle
            continue

        filtered = chunk[chunk[route_col].isin(routes)]

        # Secondary filter: keep only crashes within 8 km of a corridor waypoint
        lat_col = next((c for c in ("LATITUDE", "latitude") if c in filtered.columns), None)
        lon_col = next((c for c in ("LONGITUDE", "longitude") if c in filtered.columns), None)
        if lat_col and lon_col and not filtered.empty:
            lats = pd.to_numeric(filtered[lat_col], errors="coerce")
            lons = pd.to_numeric(filtered[lon_col], errors="coerce")
            corridor_mask = [
                is_near_corridor(lat, lon) if (lat == lat and lon == lon and lat != 0 and lon != 0) else True
                for lat, lon in zip(lats, lons)
            ]
            filtered = filtered[corridor_mask]

        if not filtered.empty:
            frames.append(filtered)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def download_ccrs(years: list[int] | None = None) -> pd.DataFrame:
    """Full download: fetch CCRS crash data for given years, filtered to corridor.

    De-duplicates on case_id across years (same crash can appear in both
    year files near year boundary). Saves combined CSV to OUTPUT_CSV.
    Skips years with unknown resource IDs with a warning.
    """
    if years is None:
        current_year = date.today().year
        years = [current_year - 1, current_year]

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    all_frames: list[pd.DataFrame] = []

    for year in sorted(years):
        resource_id = CCRS_RESOURCE_IDS.get(year)
        if not resource_id:
            log.warning(
                "ccrs_resource_id_unknown",
                year=year,
                hint="Add the resource ID to CCRS_RESOURCE_IDS in ccrs_client.py. "
                     "Find it at https://data.ca.gov/dataset/ccrs",
            )
            continue

        try:
            url = _get_download_url(resource_id)
            df_year = _stream_and_filter(url, CORRIDOR_ROUTES)
            log.info("ccrs_year_loaded", year=year, rows=len(df_year))
            all_frames.append(df_year)
        except Exception as exc:
            log.error("ccrs_year_failed", year=year, error=str(exc))

    if not all_frames:
        log.warning("ccrs_no_data_loaded")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)

    # De-duplicate on case ID if present
    if "CASE_ID" in combined.columns or "case_id" in combined.columns:
        id_col = "case_id" if "case_id" in combined.columns else "CASE_ID"
        before = len(combined)
        combined = combined.drop_duplicates(subset=[id_col])
        log.info("ccrs_dedup", removed=before - len(combined))

    combined.to_csv(OUTPUT_CSV, index=False)
    log.info("ccrs_saved", rows=len(combined), path=str(OUTPUT_CSV))
    print(f"Saved {len(combined):,} crash records -> {OUTPUT_CSV}")

    # Print a quick breakdown
    if "ROUTE" in combined.columns:
        print(combined["ROUTE"].value_counts().to_string())

    return combined


# ---------------------------------------------------------------------------
# Daily incremental update
# ---------------------------------------------------------------------------

CHECKPOINT_FILE = Path("data/switrs/.checkpoint.json")


def _read_checkpoint() -> date | None:
    """Return the last collision_date fully processed, or None."""
    if not CHECKPOINT_FILE.exists():
        return None
    try:
        payload = json.loads(CHECKPOINT_FILE.read_text())
        return date.fromisoformat(payload["last_date"])
    except Exception:
        return None


def _write_checkpoint(last_date: date) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps({"last_date": last_date.isoformat()}))
    log.info("ccrs_checkpoint_updated", last_date=last_date.isoformat())


def fetch_daily_update() -> int:
    """Incremental update — download current-year CSV, append only new records.

    Re-downloads the whole yearly file each time (~34 MB, manageable) but only
    appends rows after the checkpoint date. On first run falls back to full
    download_ccrs() so there's always a base dataset.
    Returns count of new rows added.
    """
    last_date = _read_checkpoint()

    if last_date is None:
        log.info("ccrs_no_checkpoint_running_full_download")
        download_ccrs()
        # Set checkpoint to today so tomorrow's run only fetches new crashes
        _write_checkpoint(date.today())
        return 0

    current_year = date.today().year
    resource_id = CCRS_RESOURCE_IDS.get(current_year)
    if not resource_id:
        log.warning("ccrs_current_year_resource_unknown", year=current_year)
        return 0

    try:
        url = _get_download_url(resource_id)
        fresh = _stream_and_filter(url, CORRIDOR_ROUTES)
    except Exception as exc:
        log.error("ccrs_daily_download_failed", error=str(exc))
        return 0

    if fresh.empty:
        log.info("ccrs_daily_no_new_data")
        return 0

    # Normalise date column
    date_col = next((c for c in ("COLLISION_DATE", "collision_date") if c in fresh.columns), None)
    if date_col:
        fresh[date_col] = pd.to_datetime(fresh[date_col], errors="coerce")
        new_rows = fresh[fresh[date_col].dt.date > last_date]
    else:
        new_rows = fresh  # can't filter by date; append everything

    if new_rows.empty:
        log.info("ccrs_daily_no_new_rows_since_checkpoint", last_date=last_date)
        _write_checkpoint(date.today())
        return 0

    # Append to existing crash CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_CSV.exists():
        existing = pd.read_csv(OUTPUT_CSV, dtype=str)
        id_col = next((c for c in ("case_id", "CASE_ID") if c in existing.columns), None)
        combined = pd.concat([existing, new_rows], ignore_index=True)
        if id_col and id_col in combined.columns:
            combined = combined.drop_duplicates(subset=[id_col])
    else:
        combined = new_rows

    combined.to_csv(OUTPUT_CSV, index=False)
    added = len(new_rows)
    log.info("ccrs_daily_update_complete", new_rows=added, total=len(combined))
    print(f"CCRS daily update: +{added:,} new crash records (total {len(combined):,})")

    if date_col:
        max_date = new_rows[date_col].max()
        if pd.notna(max_date):
            _write_checkpoint(max_date.date())
    else:
        _write_checkpoint(date.today())

    return added


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download CCRS crash data for corridor routes")
    parser.add_argument(
        "--years", nargs="+", type=int,
        help="Calendar years to download (default: last 2 years)",
    )
    parser.add_argument(
        "--daily", action="store_true",
        help="Run incremental daily update only (uses checkpoint)",
    )
    args = parser.parse_args()
    if args.daily:
        fetch_daily_update()
    else:
        download_ccrs(years=args.years)
