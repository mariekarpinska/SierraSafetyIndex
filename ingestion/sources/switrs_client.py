"""SWITRS crash data client for Sierra Nevada corridors.

Data source: California SWITRS (Statewide Integrated Traffic Records System)
via the TIMS Berkeley open-access export.

**One-time setup — download the CSV:**
  1. Go to https://tims.berkeley.edu/tools/query.php
  2. Select: State Highway = checked, Routes = 80, 88, 50
  3. Date range: last 3–5 years, all counties
  4. Download → CSV → save to data/switrs/switrs_crashes.csv

OR download the statewide bulk file (no account needed):
  https://iswitrs.chp.ca.gov/Reports/jsp/RawDataHTML.jsp
  (Select "Collisions", last 3 years, all of California → download zip)

The SWITRS CSV columns used here:
  ACCIDENT_YEAR, COLLISION_DATE, COLLISION_TIME,
  PRIMARY_COLLISION_FACTOR, COLLISION_SEVERITY,
  TYPE_OF_COLLISION, PARTY_TYPE, VEHICLE_TYPE,
  PCF_VIOLATION_CATEGORY, LATITUDE, LONGITUDE,
  STATE_HWY_IND, ROUTE
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import structlog

from ingestion.sources.usgs_client import _haversine_km_vec
from ingestion.sources.corridor import classify_hwy

log = structlog.get_logger()

DEFAULT_CSV_PATH = Path("data/switrs/switrs_crashes.csv")

# ---------------------------------------------------------------------------
# SWITRS severity / type mappings  (raw SWITRS codes → human labels)
# ---------------------------------------------------------------------------
SEVERITY_MAP = {
    "1": "Fatal",
    "2": "Severe Injury",
    "3": "Other Visible Injury",
    "4": "Complaint of Pain",
    "0": "Property Damage Only",
}

COLLISION_TYPE_MAP = {
    "A": "Head-On",
    "B": "Sideswipe",
    "C": "Rear-End",
    "D": "Broadside",
    "E": "Hit Object",
    "F": "Overturned",
    "G": "Vehicle/Pedestrian",
    "H": "Other",
}

PCF_MAP = {
    "A": "DUI",
    "B": "Improper Driving",
    "C": "Wrong Side of Road",
    "D": "Improper Passing",
    "E": "Unsafe Speed",
    "F": "Following Too Closely",
    "G": "Improper Turn",
    "H": "Automobile Right of Way",
    "I": "Pedestrian Right of Way",
    "J": "Pedestrian Violation",
    "K": "Traffic Signals/Signs",
    "L": "Hazardous Parking",
    "M": "Lights",
    "N": "Brakes",
    "O": "Other Equipment",
    "P": "Other Hazardous Movement",
    "Q": "Unknown",
    "R": "Fell Asleep",
    "S": "Unsafe Lane Change",
    "T": "Improper Lane Change",
    "U": "Pedestrian or Other Under Influence",
    "V": "Other Than Driver",
    "W": "Speeding",
    "X": "Unknown",
    "Y": "Fell Asleep",
    "-": "Unknown",
}

VEHICLE_TYPE_MAP = {
    "A": "Passenger Car",
    "B": "Other Bus",
    "C": "Motorcycle/Scooter",
    "D": "Bicycle",
    "E": "Truck or Truck Tractor",
    "F": "Pickup or Panel Truck",
    "G": "SUV",
    "H": "Emergency Vehicle",
    "I": "Other",
    "J": "School Bus",
    "K": "Heavy Truck",
}

# Routes to keep when filtering bulk statewide CSV
CORRIDOR_ROUTES = {"80", "88", "89", "50"}

# CCRS export uses different column names than the older SWITRS bulk export.
# After uppercasing, rename these to the SWITRS-standard names load_switrs expects.
_CCRS_ALIASES = {
    "COLLISION ID":                      "CASE_ID",
    "CRASH DATE TIME":                   "COLLISION_DATE",
    "COLLISION TYPE CODE":               "TYPE_OF_COLLISION",
    "PRIMARY COLLISION FACTOR CODE":     "PCF_VIOLATION_CATEGORY",
}

# Corridor waypoints (I-80, US-50, HWY-88) — used for spatial filtering.
# Crashes outside _CORRIDOR_RADIUS_KM of every waypoint are dropped.
_CORRIDOR_WAYPOINTS: list[tuple[float, float]] = [
    (38.5816, -121.4944),  # Sacramento
    (38.7521, -121.2880),  # Roseville
    (38.8966, -121.0769),  # Auburn
    (39.1002, -120.9533),  # Colfax
    (39.2835, -120.6715),  # Emigrant Gap
    (39.3232, -120.3253),  # Donner Summit
    (39.3280, -120.1833),  # Truckee
    (38.3490, -120.7752),  # Jackson (HWY-88)
    (38.4330, -120.5707),  # Pioneer
    (38.6868, -120.0657),  # Kirkwood
    (38.6940, -119.9800),  # Carson Pass
    (38.7296, -120.7985),  # Placerville (US-50)
    (38.8235, -120.0352),  # Echo Summit
    (38.9399, -119.9772),  # South Lake Tahoe
    # HWY-89 (South Lake Tahoe → Truckee, west shore of Lake Tahoe)
    (38.9500, -119.9800),  # South Lake Tahoe / HWY-89 junction
    (39.1706, -120.1471),  # Tahoe City
    (39.2388, -120.0272),  # Kings Beach area
    (39.3218, -120.2050),  # Truckee / HWY-89 junction
]
_CORRIDOR_RADIUS_KM = 20.0  # ~12 miles — wide enough to catch on-ramps / nearby roads


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_switrs(csv_path: Path = DEFAULT_CSV_PATH) -> pd.DataFrame:
    """Load and normalize a SWITRS or CCRS CSV into a clean DataFrame.

    Returns columns:
      case_id, collision_datetime, lat, lon, route,
      severity, collision_type, primary_factor, vehicle_type
    """
    if not csv_path.exists():
        log.warning("switrs_csv_not_found", path=str(csv_path))
        return pd.DataFrame()

    df = pd.read_csv(csv_path, low_memory=False, dtype=str)
    df.columns = df.columns.str.upper().str.strip()

    # Normalise CCRS column names to SWITRS-compatible names
    df = df.rename(columns={k: v for k, v in _CCRS_ALIASES.items() if k in df.columns})

    if "COLLISION_DATE" in df.columns:
        df["collision_datetime"] = pd.to_datetime(
            df["COLLISION_DATE"], errors="coerce"
        )
    elif "ACCIDENT_YEAR" in df.columns:
        df["collision_datetime"] = pd.to_datetime(
            df["ACCIDENT_YEAR"], format="%Y", errors="coerce"
        )
    else:
        df["collision_datetime"] = pd.NaT

    # Coordinates
    df["lat"] = pd.to_numeric(df.get("LATITUDE",  pd.Series(dtype=float)), errors="coerce")
    df["lon"] = pd.to_numeric(df.get("LONGITUDE", pd.Series(dtype=float)), errors="coerce")
    df = df.dropna(subset=["lat", "lon"])
    df = df[(df["lat"] != 0) & (df["lon"] != 0)]

    # Spatial filter: keep only crashes inside the 4 tracked corridor polygons.
    # classify_hwy() uses polygon containment (data/coords/) with waypoint fallback.
    # Crashes outside all corridors are excluded entirely.
    df["hwy"] = [classify_hwy(lat, lon) for lat, lon in zip(df["lat"], df["lon"])]
    df = df[df["hwy"].notna()]

    if df.empty:
        log.warning("switrs_all_rows_filtered", path=str(csv_path))
        return pd.DataFrame()

    # CCRS doesn't have a COLLISION_SEVERITY code — derive from killed/injured counts
    if "COLLISION_SEVERITY" not in df.columns:
        killed  = pd.to_numeric(df.get("NUMBERKILLED",  pd.Series(0, index=df.index)), errors="coerce").fillna(0)
        injured = pd.to_numeric(df.get("NUMBERINJURED", pd.Series(0, index=df.index)), errors="coerce").fillna(0)
        sev = pd.Series("0", index=df.index)       # Property Damage Only
        sev = sev.where(injured == 0, "3")          # Other Visible Injury
        sev = sev.where(killed  == 0, "1")          # Fatal
        df["COLLISION_SEVERITY"] = sev

    # Map codes to labels
    df["severity"]       = df["COLLISION_SEVERITY"].map(SEVERITY_MAP).fillna("Unknown")
    df["collision_type"] = df.get("TYPE_OF_COLLISION",  pd.Series(dtype=str)).map(COLLISION_TYPE_MAP).fillna("Unknown")
    df["primary_factor"] = df.get("PCF_VIOLATION_CATEGORY", pd.Series(dtype=str)).map(PCF_MAP).fillna("Unknown")
    df["vehicle_type"]   = df.get("VEHICLE_TYPE",       pd.Series(dtype=str)).map(VEHICLE_TYPE_MAP).fillna("Unknown")
    route_col = "ROUTE" if "ROUTE" in df.columns else None
    df["route"]          = df.get(route_col, pd.Series(dtype=str)).fillna("Unknown")
    df["case_id"]        = df.get("CASE_ID", pd.Series(dtype=str)).fillna("")

    cols = ["case_id", "collision_datetime", "lat", "lon",
            "route", "hwy", "severity", "collision_type", "primary_factor", "vehicle_type"]
    return df[[c for c in cols if c in df.columns]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Penalty computation (called by producer for live score)
# ---------------------------------------------------------------------------

def recent_crash_penalty(
    crashes_df: pd.DataFrame,
    seg_lat: float,
    seg_lon: float,
    radius_km: float = 16.0,   # ~10 miles
    hours: int = 24,
    as_of: Optional[datetime] = None,
) -> tuple[int, int, str]:
    """Return (penalty, crash_count, max_severity) for crashes near a segment.

    as_of: the point-in-time to score from.
      - Pass None (default) for live scoring → uses datetime.utcnow().
      - Pass an explicit datetime for historical backfill scoring so that
        only crashes that had already occurred by that moment are counted.
        This ensures past scores are not contaminated by future crash data.
    """
    if crashes_df.empty or "collision_datetime" not in crashes_df.columns:
        return 0, 0, "None"

    reference = as_of if as_of is not None else datetime.utcnow()
    cutoff = reference - timedelta(hours=hours)
    recent = crashes_df[
        (crashes_df["collision_datetime"] >= cutoff)
        & (crashes_df["collision_datetime"] < reference)
    ]
    if recent.empty:
        return 0, 0, "None"

    dists = _haversine_km_vec(seg_lat, seg_lon, recent["lat"].to_numpy(), recent["lon"].to_numpy())
    nearby = recent[dists <= radius_km]
    if nearby.empty:
        return 0, 0, "None"

    count = len(nearby)
    severity_order = ["Fatal", "Severe Injury", "Other Visible Injury",
                      "Complaint of Pain", "Property Damage Only", "Unknown"]
    max_sev = "Unknown"
    for sev in severity_order:
        if sev in nearby["severity"].values:
            max_sev = sev
            break

    if max_sev == "Fatal":
        penalty = 20
    elif max_sev == "Severe Injury":
        penalty = 12
    elif max_sev in ("Other Visible Injury", "Complaint of Pain"):
        penalty = 6
    else:
        penalty = 3 * min(count, 3)

    return min(penalty, 25), count, max_sev


# ---------------------------------------------------------------------------
# Dashboard helpers
# ---------------------------------------------------------------------------

def crash_stats(crashes_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return aggregated stats tables for the dashboard.

    Keys: 'by_severity', 'by_type', 'by_factor', 'by_vehicle', 'hotspots'
    """
    if crashes_df.empty:
        empty = pd.DataFrame()
        return {k: empty for k in ("by_severity", "by_type", "by_factor", "by_vehicle", "hotspots")}

    def counts(col: str) -> pd.DataFrame:
        return (
            crashes_df[col].value_counts()
            .rename_axis(col)
            .reset_index(name="count")
        )

    hotspots = (
        crashes_df.groupby(["lat", "lon", "route"])
        .agg(crash_count=("case_id", "count"), worst_severity=("severity", "first"))
        .reset_index()
        .sort_values("crash_count", ascending=False)
    )

    return {
        "by_severity": counts("severity"),
        "by_type":     counts("collision_type"),
        "by_factor":   counts("primary_factor"),
        "by_vehicle":  counts("vehicle_type"),
        "hotspots":    hotspots,
    }
