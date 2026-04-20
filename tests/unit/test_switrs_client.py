"""Unit tests for switrs_client — load_switrs, recent_crash_penalty, crash_stats."""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from ingestion.sources.switrs_client import crash_stats, load_switrs, recent_crash_penalty


def _write_csv(tmp_path: Path, rows: list[dict]) -> Path:
    p = tmp_path / "switrs_crashes.csv"
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# load_switrs
# ---------------------------------------------------------------------------

def test_load_switrs_missing_file_returns_empty(tmp_path: Path) -> None:
    assert load_switrs(tmp_path / "no_such_file.csv").empty


def test_load_switrs_corridor_crash_returned(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [{
        "COLLISION_DATE": "2024-01-15 07:30:00",
        "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
        "COLLISION_SEVERITY": "2",
        "TYPE_OF_COLLISION": "C",
        "PCF_VIOLATION_CATEGORY": "E",
        "VEHICLE_TYPE": "A",
        "ROUTE": "80",
        "CASE_ID": "C001",
    }])
    result = load_switrs(p)
    assert not result.empty
    assert result.iloc[0]["severity"] == "Severe Injury"
    assert result.iloc[0]["case_id"] == "C001"


def test_load_switrs_filters_out_of_corridor(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [{
        "COLLISION_DATE": "2024-01-15",
        "LATITUDE": "34.0", "LONGITUDE": "-118.0",
        "COLLISION_SEVERITY": "1",
        "ROUTE": "80", "CASE_ID": "C002",
    }])
    assert load_switrs(p).empty


def test_load_switrs_drops_zero_coords(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [{
        "COLLISION_DATE": "2024-01-15",
        "LATITUDE": "0", "LONGITUDE": "0",
        "COLLISION_SEVERITY": "1",
        "ROUTE": "80", "CASE_ID": "C003",
    }])
    assert load_switrs(p).empty


def test_load_switrs_accident_year_fallback(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [{
        "ACCIDENT_YEAR": "2024",
        "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
        "COLLISION_SEVERITY": "1",
        "ROUTE": "80", "CASE_ID": "C004",
    }])
    result = load_switrs(p)
    assert not result.empty
    assert pd.notna(result.iloc[0]["collision_datetime"])


def test_load_switrs_no_date_column_uses_nat(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [{
        "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
        "COLLISION_SEVERITY": "2",
        "ROUTE": "80", "CASE_ID": "C005",
    }])
    result = load_switrs(p)
    assert not result.empty
    assert pd.isna(result.iloc[0]["collision_datetime"])


def test_load_switrs_ccrs_alias_renaming(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [{
        "COLLISION ID": "X001",
        "CRASH DATE TIME": "2024-02-10 08:00:00",
        "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
        "COLLISION_SEVERITY": "2",
        "ROUTE": "80",
    }])
    result = load_switrs(p)
    assert not result.empty
    assert result.iloc[0]["case_id"] == "X001"


def test_load_switrs_derives_severity_from_killed_injured(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [{
        "COLLISION_DATE": "2024-01-15",
        "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
        "NUMBERKILLED": "1", "NUMBERINJURED": "0",
        "ROUTE": "80", "CASE_ID": "C006",
    }])
    result = load_switrs(p)
    assert not result.empty
    assert result.iloc[0]["severity"] == "Fatal"


def test_load_switrs_all_rows_filtered_returns_empty(tmp_path: Path) -> None:
    p = _write_csv(tmp_path, [
        {"COLLISION_DATE": "2024-01-15", "LATITUDE": "34.0", "LONGITUDE": "-118.0",
         "COLLISION_SEVERITY": "1", "ROUTE": "80", "CASE_ID": "C007"},
        {"COLLISION_DATE": "2024-01-15", "LATITUDE": "34.1", "LONGITUDE": "-118.1",
         "COLLISION_SEVERITY": "2", "ROUTE": "80", "CASE_ID": "C008"},
    ])
    assert load_switrs(p).empty


# ---------------------------------------------------------------------------
# recent_crash_penalty
# ---------------------------------------------------------------------------

def test_recent_crash_penalty_empty_df() -> None:
    pen, count, sev = recent_crash_penalty(pd.DataFrame(), 39.3232, -120.3253)
    assert (pen, count, sev) == (0, 0, "None")


def test_recent_crash_penalty_no_datetime_col() -> None:
    df = pd.DataFrame([{"lat": 39.3232, "lon": -120.3253, "severity": "Fatal"}])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253)
    assert (pen, count, sev) == (0, 0, "None")


def test_recent_crash_penalty_no_recent_crashes() -> None:
    now = datetime(2024, 1, 15, 12, 0)
    df = pd.DataFrame([{
        "collision_datetime": now - timedelta(hours=48),
        "lat": 39.3232, "lon": -120.3253, "severity": "Fatal",
    }])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253, hours=24, as_of=now)
    assert (pen, count, sev) == (0, 0, "None")


def test_recent_crash_penalty_no_nearby_crashes() -> None:
    now = datetime(2024, 1, 15, 12, 0)
    df = pd.DataFrame([{
        "collision_datetime": now - timedelta(hours=1),
        "lat": 34.0, "lon": -118.0, "severity": "Fatal",
    }])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253, as_of=now)
    assert (pen, count, sev) == (0, 0, "None")


def test_recent_crash_penalty_fatal() -> None:
    now = datetime(2024, 1, 15, 12, 0)
    df = pd.DataFrame([{
        "collision_datetime": now - timedelta(hours=1),
        "lat": 39.3232, "lon": -120.3253, "severity": "Fatal",
    }])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253, as_of=now)
    assert pen == 20
    assert sev == "Fatal"


def test_recent_crash_penalty_severe_injury() -> None:
    now = datetime(2024, 1, 15, 12, 0)
    df = pd.DataFrame([{
        "collision_datetime": now - timedelta(hours=2),
        "lat": 39.3232, "lon": -120.3253, "severity": "Severe Injury",
    }])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253, as_of=now)
    assert pen == 12


def test_recent_crash_penalty_visible_injury() -> None:
    now = datetime(2024, 1, 15, 12, 0)
    df = pd.DataFrame([{
        "collision_datetime": now - timedelta(hours=2),
        "lat": 39.3232, "lon": -120.3253, "severity": "Other Visible Injury",
    }])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253, as_of=now)
    assert pen == 6


def test_recent_crash_penalty_complaint_of_pain() -> None:
    now = datetime(2024, 1, 15, 12, 0)
    df = pd.DataFrame([{
        "collision_datetime": now - timedelta(hours=2),
        "lat": 39.3232, "lon": -120.3253, "severity": "Complaint of Pain",
    }])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253, as_of=now)
    assert pen == 6


def test_recent_crash_penalty_property_damage_capped_at_25() -> None:
    now = datetime(2024, 1, 15, 12, 0)
    rows = [{
        "collision_datetime": now - timedelta(hours=1),
        "lat": 39.3232, "lon": -120.3253, "severity": "Property Damage Only",
    } for _ in range(10)]
    df = pd.DataFrame(rows)
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253, as_of=now)
    assert pen <= 25


def test_recent_crash_penalty_uses_utcnow_when_no_as_of() -> None:
    df = pd.DataFrame([{
        "collision_datetime": datetime.utcnow() - timedelta(hours=1),
        "lat": 39.3232, "lon": -120.3253, "severity": "Fatal",
    }])
    pen, count, sev = recent_crash_penalty(df, 39.3232, -120.3253)
    assert pen == 20


# ---------------------------------------------------------------------------
# crash_stats
# ---------------------------------------------------------------------------

def test_crash_stats_empty_df() -> None:
    stats = crash_stats(pd.DataFrame())
    assert set(stats.keys()) == {"by_severity", "by_type", "by_factor", "by_vehicle", "hotspots"}
    assert all(v.empty for v in stats.values())


def test_crash_stats_with_data() -> None:
    df = pd.DataFrame([{
        "case_id": "C001",
        "collision_datetime": datetime(2024, 1, 15),
        "lat": 39.3232, "lon": -120.3253,
        "route": "80",
        "severity": "Fatal",
        "collision_type": "Head-On",
        "primary_factor": "DUI",
        "vehicle_type": "Passenger Car",
    }])
    stats = crash_stats(df)
    assert not stats["by_severity"].empty
    assert not stats["hotspots"].empty
    assert stats["by_severity"].iloc[0]["count"] == 1
    assert stats["hotspots"].iloc[0]["crash_count"] == 1
