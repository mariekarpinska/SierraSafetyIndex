"""Unit tests for ingestion/backfill.py — pure helpers and mocked HTTP."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import responses as responses_lib

from ingestion.backfill import (
    OPENMETEO_ARCHIVE_URL,
    USGS_CATALOG_URL,
    _crash_penalty_at,
    _infer_chain_control,
    _load_crashes_for_backfill,
    _load_to_bigquery,
    _nearest_quake_mag,
    _widen_bq_schema,
    backfill,
    fetch_historical_quakes,
    fetch_historical_weather,
)


# ---------------------------------------------------------------------------
# _infer_chain_control — pure function
# ---------------------------------------------------------------------------

def test_infer_chain_control_none_when_light() -> None:
    assert _infer_chain_control(0.1) is None


def test_infer_chain_control_r1() -> None:
    assert _infer_chain_control(0.25) == "R1"


def test_infer_chain_control_r2() -> None:
    assert _infer_chain_control(0.6) == "R2"


def test_infer_chain_control_r3() -> None:
    assert _infer_chain_control(1.5) == "R3"


# ---------------------------------------------------------------------------
# _nearest_quake_mag
# ---------------------------------------------------------------------------

def _make_quakes_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["eq_time", "eq_lat", "eq_lon", "eq_mag"]
    )


def test_nearest_quake_mag_empty_df_returns_none() -> None:
    ts = datetime(2024, 1, 15, 12)
    assert _nearest_quake_mag(_make_quakes_df([]), ts, 39.3232, -120.3253) is None


def test_nearest_quake_mag_within_window_and_radius() -> None:
    ts = datetime(2024, 1, 15, 12)
    quakes = _make_quakes_df([{
        "eq_time": ts - timedelta(hours=2),
        "eq_lat": 39.3232,
        "eq_lon": -120.3253,
        "eq_mag": 3.5,
    }])
    result = _nearest_quake_mag(quakes, ts, 39.3232, -120.3253)
    assert result == 3.5


def test_nearest_quake_mag_outside_time_window() -> None:
    ts = datetime(2024, 1, 15, 12)
    quakes = _make_quakes_df([{
        "eq_time": ts - timedelta(hours=10),
        "eq_lat": 39.3232, "eq_lon": -120.3253, "eq_mag": 4.0,
    }])
    assert _nearest_quake_mag(quakes, ts, 39.3232, -120.3253) is None


def test_nearest_quake_mag_outside_radius() -> None:
    ts = datetime(2024, 1, 15, 12)
    quakes = _make_quakes_df([{
        "eq_time": ts - timedelta(hours=1),
        "eq_lat": 34.0, "eq_lon": -118.0, "eq_mag": 5.0,
    }])
    assert _nearest_quake_mag(quakes, ts, 39.3232, -120.3253) is None


# ---------------------------------------------------------------------------
# _load_crashes_for_backfill
# ---------------------------------------------------------------------------

def test_load_crashes_for_backfill_missing_csv(tmp_path: Path) -> None:
    with patch("ingestion.backfill.CRASHES_CSV", tmp_path / "missing.csv"):
        result = _load_crashes_for_backfill()
    assert result == {}


def test_load_crashes_for_backfill_no_date_col(tmp_path: Path) -> None:
    csv = tmp_path / "crashes.csv"
    pd.DataFrame([{"LATITUDE": "39.0", "LONGITUDE": "-120.0"}]).to_csv(csv, index=False)
    with patch("ingestion.backfill.CRASHES_CSV", csv):
        result = _load_crashes_for_backfill()
    assert result == {}


def test_load_crashes_for_backfill_with_valid_data(tmp_path: Path) -> None:
    csv = tmp_path / "crashes.csv"
    pd.DataFrame([{
        "COLLISION_DATE": "2024-01-15",
        "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
        "COLLISION_SEVERITY": "1",
    }]).to_csv(csv, index=False)
    with patch("ingestion.backfill.CRASHES_CSV", csv):
        result = _load_crashes_for_backfill()
    assert len(result) > 0


def test_load_crashes_for_backfill_drops_zero_coords(tmp_path: Path) -> None:
    csv = tmp_path / "crashes.csv"
    pd.DataFrame([{
        "COLLISION_DATE": "2024-01-15",
        "LATITUDE": "0", "LONGITUDE": "0",
        "COLLISION_SEVERITY": "1",
    }]).to_csv(csv, index=False)
    with patch("ingestion.backfill.CRASHES_CSV", csv):
        result = _load_crashes_for_backfill()
    assert result == {}


def test_load_crashes_no_severity_col(tmp_path: Path) -> None:
    csv = tmp_path / "crashes.csv"
    pd.DataFrame([{
        "COLLISION_DATE": "2024-01-15",
        "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
    }]).to_csv(csv, index=False)
    with patch("ingestion.backfill.CRASHES_CSV", csv):
        result = _load_crashes_for_backfill()
    assert len(result) > 0


# ---------------------------------------------------------------------------
# _crash_penalty_at
# ---------------------------------------------------------------------------

def test_crash_penalty_at_empty_buckets() -> None:
    ts = datetime(2024, 1, 15, 12)
    pen, count, sev = _crash_penalty_at({}, ts, 39.3232, -120.3253)
    assert (pen, count, sev) == (0, 0, "None")


def test_crash_penalty_at_no_matching_day() -> None:
    ts = datetime(2024, 1, 15, 12)
    buckets = {date(2023, 12, 1): pd.DataFrame([{
        "_crash_dt": datetime(2023, 12, 1, 12),
        "_lat": 39.3232, "_lon": -120.3253, "_severity": "Fatal",
    }])}
    pen, count, sev = _crash_penalty_at(buckets, ts, 39.3232, -120.3253)
    assert (pen, count, sev) == (0, 0, "None")


def test_crash_penalty_at_fatal_in_window() -> None:
    ts = datetime(2024, 1, 15, 12)
    crash_dt = ts - timedelta(hours=2)
    buckets = {crash_dt.date(): pd.DataFrame([{
        "_crash_dt": pd.Timestamp(crash_dt),
        "_lat": 39.3232, "_lon": -120.3253, "_severity": "Fatal",
    }])}
    pen, count, sev = _crash_penalty_at(buckets, ts, 39.3232, -120.3253)
    assert pen == 20
    assert sev == "Fatal"


def test_crash_penalty_at_severe_injury() -> None:
    ts = datetime(2024, 1, 15, 12)
    crash_dt = ts - timedelta(hours=3)
    buckets = {crash_dt.date(): pd.DataFrame([{
        "_crash_dt": pd.Timestamp(crash_dt),
        "_lat": 39.3232, "_lon": -120.3253, "_severity": "Severe Injury",
    }])}
    pen, count, sev = _crash_penalty_at(buckets, ts, 39.3232, -120.3253)
    assert pen == 12


def test_crash_penalty_at_visible_or_pain() -> None:
    ts = datetime(2024, 1, 15, 12)
    crash_dt = ts - timedelta(hours=1)
    for sev_label in ("Other Visible Injury", "Complaint of Pain"):
        buckets = {crash_dt.date(): pd.DataFrame([{
            "_crash_dt": pd.Timestamp(crash_dt),
            "_lat": 39.3232, "_lon": -120.3253, "_severity": sev_label,
        }])}
        pen, count, sev = _crash_penalty_at(buckets, ts, 39.3232, -120.3253)
        assert pen == 6


def test_crash_penalty_at_property_damage() -> None:
    ts = datetime(2024, 1, 15, 12)
    crash_dt = ts - timedelta(hours=1)
    buckets = {crash_dt.date(): pd.DataFrame([{
        "_crash_dt": pd.Timestamp(crash_dt),
        "_lat": 39.3232, "_lon": -120.3253, "_severity": "Property Damage Only",
    }])}
    pen, count, sev = _crash_penalty_at(buckets, ts, 39.3232, -120.3253)
    assert pen == 3


def test_crash_penalty_at_no_nearby_crashes() -> None:
    ts = datetime(2024, 1, 15, 12)
    crash_dt = ts - timedelta(hours=1)
    buckets = {crash_dt.date(): pd.DataFrame([{
        "_crash_dt": pd.Timestamp(crash_dt),
        "_lat": 34.0, "_lon": -118.0, "_severity": "Fatal",
    }])}
    pen, count, sev = _crash_penalty_at(buckets, ts, 39.3232, -120.3253, radius_km=1.0)
    assert (pen, count, sev) == (0, 0, "None")


def test_crash_penalty_at_outside_temporal_window() -> None:
    ts = datetime(2024, 1, 15, 12)
    crash_dt = ts - timedelta(hours=30)
    buckets = {crash_dt.date(): pd.DataFrame([{
        "_crash_dt": pd.Timestamp(crash_dt),
        "_lat": 39.3232, "_lon": -120.3253, "_severity": "Fatal",
    }])}
    pen, count, sev = _crash_penalty_at(buckets, ts, 39.3232, -120.3253)
    assert (pen, count, sev) == (0, 0, "None")


# ---------------------------------------------------------------------------
# fetch_historical_weather (mocked HTTP)
# ---------------------------------------------------------------------------

@responses_lib.activate
def test_fetch_historical_weather_returns_dataframe() -> None:
    payload = {
        "hourly": {
            "time": ["2024-01-15T00:00", "2024-01-15T01:00"],
            "snowfall": [0.5, 1.2],
            "wind_gusts_10m": [25.0, 30.0],
            "visibility": [5000.0, 3000.0],
            "surface_temperature": [-3.0, -5.0],
        }
    }
    responses_lib.add(responses_lib.GET, OPENMETEO_ARCHIVE_URL, json=payload, status=200)

    result = fetch_historical_weather(39.3232, -120.3253, date(2024, 1, 15), date(2024, 1, 16))

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "snowfall_cm" in result.columns


# ---------------------------------------------------------------------------
# fetch_historical_quakes (mocked HTTP)
# ---------------------------------------------------------------------------

@responses_lib.activate
def test_fetch_historical_quakes_returns_dataframe() -> None:
    payload = {
        "features": [{
            "properties": {"time": 1705318800000, "mag": 3.2},
            "geometry": {"coordinates": [-120.4, 39.3, 5.0]},
        }]
    }
    responses_lib.add(responses_lib.GET, USGS_CATALOG_URL, json=payload, status=200)

    result = fetch_historical_quakes(date(2024, 1, 15), date(2024, 1, 16))

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["eq_mag"] == 3.2


@responses_lib.activate
def test_fetch_historical_quakes_empty_returns_empty_df() -> None:
    responses_lib.add(responses_lib.GET, USGS_CATALOG_URL, json={"features": []}, status=200)

    result = fetch_historical_quakes(date(2024, 1, 15), date(2024, 1, 16))

    assert isinstance(result, pd.DataFrame)
    assert result.empty


# ---------------------------------------------------------------------------
# backfill integration (mocked network + filesystem)
# ---------------------------------------------------------------------------

def _weather_df() -> pd.DataFrame:
    return pd.DataFrame([{
        "time": pd.Timestamp("2024-01-15T06:00"),
        "snowfall_cm": 2.0,
        "wind_gusts_kmh": 40.0,
        "visibility_m": 5000.0,
        "surface_temp_c": -3.0,
    }])


def _empty_quakes_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["eq_time", "eq_lat", "eq_lon", "eq_mag"])


def test_backfill_writes_parquet(tmp_path: Path) -> None:
    with patch("ingestion.backfill.fetch_historical_weather", return_value=_weather_df()), \
         patch("ingestion.backfill.fetch_historical_quakes", return_value=_empty_quakes_df()), \
         patch("ingestion.backfill.OUTPUT_DIR", tmp_path):
        backfill(
            start=date(2024, 1, 15),
            end=date(2024, 1, 15),
            segments=[{
                "segment_id": "SEG_06", "segment_name": "Donner_Summit",
                "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
            }],
        )
    parquet_files = list(tmp_path.glob("*.parquet"))
    assert len(parquet_files) == 1


def test_backfill_skips_segment_on_weather_error(tmp_path: Path) -> None:
    def bad_weather(*_args, **_kwargs):
        raise RuntimeError("network error")

    with patch("ingestion.backfill.fetch_historical_weather", side_effect=bad_weather), \
         patch("ingestion.backfill.fetch_historical_quakes", return_value=_empty_quakes_df()), \
         patch("ingestion.backfill.OUTPUT_DIR", tmp_path):
        backfill(
            start=date(2024, 1, 15),
            end=date(2024, 1, 15),
            segments=[{
                "segment_id": "SEG_06", "segment_name": "Donner_Summit",
                "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
            }],
        )
    assert list(tmp_path.glob("*.parquet")) == []


def test_backfill_with_crashes_flag(tmp_path: Path) -> None:
    crash_buckets = {}
    with patch("ingestion.backfill.fetch_historical_weather", return_value=_weather_df()), \
         patch("ingestion.backfill.fetch_historical_quakes", return_value=_empty_quakes_df()), \
         patch("ingestion.backfill._load_crashes_for_backfill", return_value=crash_buckets), \
         patch("ingestion.backfill.OUTPUT_DIR", tmp_path):
        backfill(
            start=date(2024, 1, 15),
            end=date(2024, 1, 15),
            segments=[{
                "segment_id": "SEG_06", "segment_name": "Donner_Summit",
                "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
            }],
            with_crashes=True,
        )
    assert list(tmp_path.glob("*.parquet")) != []


def test_backfill_skips_nat_timestamps(tmp_path: Path) -> None:
    wx_with_nat = pd.DataFrame([{
        "time": pd.NaT,
        "snowfall_cm": 0.0, "wind_gusts_kmh": 10.0,
        "visibility_m": 10000.0, "surface_temp_c": 5.0,
    }])
    with patch("ingestion.backfill.fetch_historical_weather", return_value=wx_with_nat), \
         patch("ingestion.backfill.fetch_historical_quakes", return_value=_empty_quakes_df()), \
         patch("ingestion.backfill.OUTPUT_DIR", tmp_path):
        backfill(
            start=date(2024, 1, 15),
            end=date(2024, 1, 15),
            segments=[{
                "segment_id": "SEG_06", "segment_name": "Donner_Summit",
                "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
            }],
        )
    assert list(tmp_path.glob("*.parquet")) == []


def test_backfill_to_bq_calls_load(tmp_path: Path) -> None:
    with patch("ingestion.backfill.fetch_historical_weather", return_value=_weather_df()), \
         patch("ingestion.backfill.fetch_historical_quakes", return_value=_empty_quakes_df()), \
         patch("ingestion.backfill._load_to_bigquery") as mock_bq, \
         patch("ingestion.backfill.OUTPUT_DIR", tmp_path):
        backfill(
            start=date(2024, 1, 15),
            end=date(2024, 1, 15),
            segments=[{
                "segment_id": "SEG_06", "segment_name": "Donner_Summit",
                "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
            }],
            to_bq=True,
        )
    mock_bq.assert_called_once()


def test_backfill_default_segments_uses_all_segments(tmp_path: Path) -> None:
    """backfill(segments=None) defaults to ALL_SEGMENTS without error."""
    from ingestion.backfill import ALL_SEGMENTS

    with patch("ingestion.backfill.fetch_historical_weather", side_effect=RuntimeError("skip")), \
         patch("ingestion.backfill.fetch_historical_quakes", return_value=_empty_quakes_df()), \
         patch("ingestion.backfill.OUTPUT_DIR", tmp_path):
        backfill(start=date(2024, 1, 15), end=date(2024, 1, 15), segments=None)

    # All segments skipped due to mocked weather error — no parquet written
    assert list(tmp_path.glob("*.parquet")) == []


def test_backfill_no_rows_produced_returns_early(tmp_path: Path) -> None:
    wx_all_nat = pd.DataFrame([{
        "time": pd.NaT, "snowfall_cm": 0.0, "wind_gusts_kmh": 0.0,
        "visibility_m": 0.0, "surface_temp_c": 0.0,
    }])
    with patch("ingestion.backfill.fetch_historical_weather", return_value=wx_all_nat), \
         patch("ingestion.backfill.fetch_historical_quakes", return_value=_empty_quakes_df()), \
         patch("ingestion.backfill.OUTPUT_DIR", tmp_path):
        backfill(date(2024, 1, 15), date(2024, 1, 15), segments=[{
            "segment_id": "SEG_06", "segment_name": "Donner_Summit",
            "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
        }])
    assert list(tmp_path.glob("*.parquet")) == []


# ---------------------------------------------------------------------------
# _widen_bq_schema
# ---------------------------------------------------------------------------

def _gcp_sys_modules(mock_bq=None, mock_storage=None, not_found_cls=None):
    if mock_bq is None:
        mock_bq = MagicMock()
    if mock_storage is None:
        mock_storage = MagicMock()
    if not_found_cls is None:
        not_found_cls = type("NotFound", (Exception,), {})
    mock_exc = MagicMock()
    mock_exc.NotFound = not_found_cls
    return {
        "google": MagicMock(),
        "google.cloud": MagicMock(bigquery=mock_bq, storage=mock_storage),
        "google.cloud.bigquery": mock_bq,
        "google.cloud.storage": mock_storage,
        "google.api_core": MagicMock(),
        "google.api_core.exceptions": mock_exc,
    }, mock_bq, mock_storage, not_found_cls


def test_widen_bq_schema_table_not_found() -> None:
    not_found_cls = type("NotFound", (Exception,), {})
    mods, mock_bq, _, _ = _gcp_sys_modules(not_found_cls=not_found_cls)

    mock_bq_client = MagicMock()
    mock_bq_client.get_table.side_effect = not_found_cls()

    with patch.dict("sys.modules", mods):
        _widen_bq_schema(mock_bq_client, "proj.ds.table", ["lat", "lon"])

    mock_bq_client.query.assert_not_called()


def test_widen_bq_schema_no_integer_columns() -> None:
    mods, _, _, _ = _gcp_sys_modules()

    mock_field = MagicMock()
    mock_field.name = "lat"
    mock_field.field_type = "FLOAT64"
    mock_table = MagicMock()
    mock_table.schema = [mock_field]
    mock_bq_client = MagicMock()
    mock_bq_client.get_table.return_value = mock_table

    with patch.dict("sys.modules", mods):
        _widen_bq_schema(mock_bq_client, "proj.ds.table", ["lat"])

    mock_bq_client.query.assert_not_called()


def test_widen_bq_schema_widens_integer_column() -> None:
    mods, _, _, _ = _gcp_sys_modules()

    mock_field = MagicMock()
    mock_field.name = "lat"
    mock_field.field_type = "INTEGER"
    mock_table = MagicMock()
    mock_table.schema = [mock_field]
    mock_bq_client = MagicMock()
    mock_bq_client.get_table.return_value = mock_table

    with patch.dict("sys.modules", mods):
        _widen_bq_schema(mock_bq_client, "proj.ds.table", ["lat"])

    mock_bq_client.query.assert_called_once()


# ---------------------------------------------------------------------------
# _load_to_bigquery
# ---------------------------------------------------------------------------

def _sample_bq_df() -> pd.DataFrame:
    return pd.DataFrame([{
        "segment_id": "SEG_06",
        "segment_name": "Donner_Summit",
        "route": "I-80",
        "lat": 39.3232,
        "lon": -120.3253,
        "elevation_ft": 7227,
        "event_timestamp": pd.Timestamp("2024-01-15T06:00:00", tz="UTC"),
        "score": 85,
        "band": "GREEN",
        "chain_control": None,
        "road_closed": None,
        "snowfall_rate_in_hr": 0.5,
        "visibility_miles": 2.0,
        "wind_gust_mph": 20.0,
        "surface_temp_c": -3.0,
        "seismic_mag": None,
    }])


def test_load_to_bigquery_direct_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.delenv("GCS_RAW_BUCKET", raising=False)
    monkeypatch.delenv("BIGQUERY_DATASET", raising=False)

    mock_bq = MagicMock()
    mods, _, _, _ = _gcp_sys_modules(mock_bq=mock_bq)

    with patch.dict("sys.modules", mods), \
         patch("ingestion.backfill._widen_bq_schema"):
        _load_to_bigquery(_sample_bq_df(), chunk_size=10)

    mock_bq.Client.return_value.load_table_from_dataframe.assert_called()


def test_load_to_bigquery_gcs_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("GCS_RAW_BUCKET", "my-bucket")
    monkeypatch.delenv("BIGQUERY_DATASET", raising=False)

    mock_bq = MagicMock()
    mock_storage = MagicMock()
    mods, _, _, _ = _gcp_sys_modules(mock_bq=mock_bq, mock_storage=mock_storage)

    with patch.dict("sys.modules", mods), \
         patch("ingestion.backfill._widen_bq_schema"), \
         patch("ingestion.backfill.Path", wraps=Path) as _mock_path:
        _load_to_bigquery(_sample_bq_df(), chunk_size=10)

    mock_bq.Client.return_value.load_table_from_uri.assert_called()
    mock_storage.Client.return_value.bucket.assert_called_with("my-bucket")


