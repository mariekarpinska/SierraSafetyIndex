"""Unit tests for ccrs_client — CKAN resolution, stream filter, download, daily update."""
from __future__ import annotations

import io
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import responses as responses_lib

from ingestion.sources.ccrs_client import (
    CKAN_API,
    CCRS_RESOURCE_IDS,
    _get_download_url,
    _read_checkpoint,
    _stream_and_filter,
    _write_checkpoint,
    download_ccrs,
    fetch_daily_update,
)


# ---------------------------------------------------------------------------
# _get_download_url
# ---------------------------------------------------------------------------

@responses_lib.activate
def test_get_download_url_returns_url() -> None:
    resource_id = "test-resource-id"
    responses_lib.add(
        responses_lib.GET,
        f"{CKAN_API}/resource_show",
        json={"result": {"url": "https://data.ca.gov/file.csv"}},
        status=200,
    )
    url = _get_download_url(resource_id)
    assert url == "https://data.ca.gov/file.csv"


@responses_lib.activate
def test_get_download_url_raises_when_no_url() -> None:
    responses_lib.add(
        responses_lib.GET,
        f"{CKAN_API}/resource_show",
        json={"result": {}},
        status=200,
    )
    with pytest.raises(ValueError, match="No URL found"):
        _get_download_url("missing-url-resource")


# ---------------------------------------------------------------------------
# _stream_and_filter
# ---------------------------------------------------------------------------

def _make_streaming_response(csv_data: str) -> MagicMock:
    class _FakeRaw(io.BytesIO):
        decode_content = False

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.raw = _FakeRaw(csv_data.encode())
    return resp


def test_stream_and_filter_corridor_route_kept() -> None:
    csv = "ROUTE,LATITUDE,LONGITUDE,CASE_ID\n80,39.3232,-120.3253,C001\n"
    resp = _make_streaming_response(csv)
    with patch("requests.get", return_value=resp):
        result = _stream_and_filter("http://example.com/data.csv", {"80"})
    assert not result.empty
    assert result.iloc[0]["case_id"] == "C001"


def test_stream_and_filter_non_corridor_route_dropped() -> None:
    csv = "ROUTE,LATITUDE,LONGITUDE,CASE_ID\n99,37.7749,-122.4194,C002\n"
    resp = _make_streaming_response(csv)
    with patch("requests.get", return_value=resp):
        result = _stream_and_filter("http://example.com/data.csv", {"80"})
    assert result.empty


def test_stream_and_filter_no_route_col_keeps_all() -> None:
    csv = "LATITUDE,LONGITUDE,CASE_ID\n39.3232,-120.3253,C003\n"
    resp = _make_streaming_response(csv)
    with patch("requests.get", return_value=resp):
        result = _stream_and_filter("http://example.com/data.csv", {"80"})
    assert not result.empty


def test_stream_and_filter_out_of_corridor_proximity_dropped() -> None:
    csv = "ROUTE,LATITUDE,LONGITUDE,CASE_ID\n80,34.0,-118.0,C004\n"
    resp = _make_streaming_response(csv)
    with patch("requests.get", return_value=resp):
        result = _stream_and_filter("http://example.com/data.csv", {"80"})
    assert result.empty


def test_stream_and_filter_empty_response_returns_empty_df() -> None:
    csv = "ROUTE,LATITUDE,LONGITUDE,CASE_ID\n"
    resp = _make_streaming_response(csv)
    with patch("requests.get", return_value=resp):
        result = _stream_and_filter("http://example.com/data.csv", {"80"})
    assert result.empty


def test_stream_and_filter_ccrs_col_map_applied() -> None:
    csv = "STATE_ROUTE,LATITUDE,LONGITUDE,CASE_ID\n80,39.3232,-120.3253,C005\n"
    resp = _make_streaming_response(csv)
    with patch("requests.get", return_value=resp):
        result = _stream_and_filter("http://example.com/data.csv", {"80"})
    assert not result.empty


# ---------------------------------------------------------------------------
# download_ccrs
# ---------------------------------------------------------------------------

def test_download_ccrs_skips_unknown_year(tmp_path: Path) -> None:
    with patch("ingestion.sources.ccrs_client.OUTPUT_CSV", tmp_path / "out.csv"), \
         patch("ingestion.sources.ccrs_client.log") as mock_log:
        result = download_ccrs(years=[1900])
    assert result.empty
    mock_log.warning.assert_called()


def test_download_ccrs_returns_dataframe(tmp_path: Path) -> None:
    csv = "ROUTE,LATITUDE,LONGITUDE,CASE_ID,COLLISION_SEVERITY\n80,39.3232,-120.3253,C001,2\n"

    def fake_get_url(_rid):
        return "http://fake.url/data.csv"

    def fake_stream(_url, _routes):
        return pd.DataFrame([{
            "ROUTE": "80", "LATITUDE": "39.3232", "LONGITUDE": "-120.3253",
            "case_id": "C001", "COLLISION_SEVERITY": "2",
        }])

    with patch("ingestion.sources.ccrs_client.OUTPUT_CSV", tmp_path / "out.csv"), \
         patch("ingestion.sources.ccrs_client._get_download_url", side_effect=fake_get_url), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", side_effect=fake_stream):
        result = download_ccrs(years=[2025])
    assert not result.empty


def test_download_ccrs_deduplicates_case_id(tmp_path: Path) -> None:
    dup_df = pd.DataFrame([
        {"case_id": "C001", "ROUTE": "80"},
        {"case_id": "C001", "ROUTE": "80"},
    ])

    def fake_get_url(_rid):
        return "http://fake.url/data.csv"

    def fake_stream(_url, _routes):
        return dup_df.copy()

    with patch("ingestion.sources.ccrs_client.OUTPUT_CSV", tmp_path / "out.csv"), \
         patch("ingestion.sources.ccrs_client._get_download_url", side_effect=fake_get_url), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", side_effect=fake_stream):
        result = download_ccrs(years=[2025])
    assert len(result) == 1


def test_download_ccrs_default_years_uses_current_and_previous(tmp_path: Path) -> None:
    """download_ccrs(years=None) defaults to current_year - 1 and current_year."""
    from datetime import date

    called_years: list[int] = []

    def fake_get_url(_rid):
        return "http://fake.url/data.csv"

    def fake_stream(_url, _routes):
        return pd.DataFrame()

    with patch("ingestion.sources.ccrs_client.OUTPUT_CSV", tmp_path / "out.csv"), \
         patch("ingestion.sources.ccrs_client._get_download_url", side_effect=fake_get_url), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", side_effect=fake_stream):
        result = download_ccrs(years=None)

    assert result.empty  # both years return empty frames


def test_download_ccrs_handles_fetch_error(tmp_path: Path) -> None:
    def fake_get_url(_rid):
        raise RuntimeError("network error")

    with patch("ingestion.sources.ccrs_client.OUTPUT_CSV", tmp_path / "out.csv"), \
         patch("ingestion.sources.ccrs_client._get_download_url", side_effect=fake_get_url):
        result = download_ccrs(years=[2025])
    assert result.empty


# ---------------------------------------------------------------------------
# _read_checkpoint / _write_checkpoint
# ---------------------------------------------------------------------------

def test_read_checkpoint_returns_none_when_missing(tmp_path: Path) -> None:
    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", tmp_path / ".checkpoint.json"):
        assert _read_checkpoint() is None


def test_write_then_read_checkpoint(tmp_path: Path) -> None:
    cp_file = tmp_path / ".checkpoint.json"
    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file):
        _write_checkpoint(date(2024, 3, 15))
        result = _read_checkpoint()
    assert result == date(2024, 3, 15)


def test_read_checkpoint_returns_none_on_corrupt_file(tmp_path: Path) -> None:
    cp_file = tmp_path / ".checkpoint.json"
    cp_file.write_text("not json at all")
    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file):
        assert _read_checkpoint() is None


# ---------------------------------------------------------------------------
# fetch_daily_update
# ---------------------------------------------------------------------------

def test_fetch_daily_update_no_checkpoint_runs_full_download(tmp_path: Path) -> None:
    cp_file = tmp_path / ".checkpoint.json"
    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.OUTPUT_CSV", tmp_path / "out.csv"), \
         patch("ingestion.sources.ccrs_client.download_ccrs") as mock_dl, \
         patch("ingestion.sources.ccrs_client._write_checkpoint"):
        result = fetch_daily_update()
    mock_dl.assert_called_once()
    assert result == 0


def test_fetch_daily_update_unknown_year_returns_zero(tmp_path: Path) -> None:
    cp_file = tmp_path / ".cp.json"
    cp_file.parent.mkdir(parents=True, exist_ok=True)
    cp_file.write_text(json.dumps({"last_date": "2024-01-01"}))

    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.CCRS_RESOURCE_IDS", {}):
        result = fetch_daily_update()
    assert result == 0


def test_fetch_daily_update_download_failure_returns_zero(tmp_path: Path) -> None:
    cp_file = tmp_path / ".cp.json"
    cp_file.write_text(json.dumps({"last_date": "2024-01-01"}))
    current_year = date.today().year
    fake_ids = {current_year: "some-resource-id"}

    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.CCRS_RESOURCE_IDS", fake_ids), \
         patch("ingestion.sources.ccrs_client._get_download_url", side_effect=RuntimeError("fail")):
        result = fetch_daily_update()
    assert result == 0


def test_fetch_daily_update_no_new_data_returns_zero(tmp_path: Path) -> None:
    cp_file = tmp_path / ".cp.json"
    cp_file.write_text(json.dumps({"last_date": "2024-01-01"}))
    current_year = date.today().year
    fake_ids = {current_year: "some-resource-id"}

    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.CCRS_RESOURCE_IDS", fake_ids), \
         patch("ingestion.sources.ccrs_client._get_download_url", return_value="http://x.com/f.csv"), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", return_value=pd.DataFrame()):
        result = fetch_daily_update()
    assert result == 0


def test_fetch_daily_update_appends_new_rows(tmp_path: Path) -> None:
    cp_file = tmp_path / ".cp.json"
    cp_file.write_text(json.dumps({"last_date": "2024-01-01"}))
    out_csv = tmp_path / "out.csv"
    current_year = date.today().year
    fake_ids = {current_year: "some-resource-id"}

    fresh_df = pd.DataFrame([{
        "COLLISION_DATE": "2024-06-01",
        "case_id": "NEW001",
        "ROUTE": "80",
    }])

    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.OUTPUT_CSV", out_csv), \
         patch("ingestion.sources.ccrs_client.CCRS_RESOURCE_IDS", fake_ids), \
         patch("ingestion.sources.ccrs_client._get_download_url", return_value="http://x.com/f.csv"), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", return_value=fresh_df), \
         patch("ingestion.sources.ccrs_client._write_checkpoint"):
        result = fetch_daily_update()
    assert result > 0


def test_fetch_daily_update_no_date_col_appends_all(tmp_path: Path) -> None:
    cp_file = tmp_path / ".cp.json"
    cp_file.write_text(json.dumps({"last_date": "2024-01-01"}))
    out_csv = tmp_path / "out.csv"
    current_year = date.today().year
    fake_ids = {current_year: "some-resource-id"}

    fresh_df = pd.DataFrame([{"case_id": "X001", "ROUTE": "80"}])

    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.OUTPUT_CSV", out_csv), \
         patch("ingestion.sources.ccrs_client.CCRS_RESOURCE_IDS", fake_ids), \
         patch("ingestion.sources.ccrs_client._get_download_url", return_value="http://x.com/f.csv"), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", return_value=fresh_df), \
         patch("ingestion.sources.ccrs_client._write_checkpoint"):
        result = fetch_daily_update()
    assert result > 0


def test_fetch_daily_update_no_rows_since_checkpoint(tmp_path: Path) -> None:
    cp_file = tmp_path / ".cp.json"
    cp_file.write_text(json.dumps({"last_date": "2099-01-01"}))
    current_year = date.today().year
    fake_ids = {current_year: "some-resource-id"}

    fresh_df = pd.DataFrame([{"COLLISION_DATE": "2024-01-01", "case_id": "OLD", "ROUTE": "80"}])

    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.CCRS_RESOURCE_IDS", fake_ids), \
         patch("ingestion.sources.ccrs_client._get_download_url", return_value="http://x.com/f.csv"), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", return_value=fresh_df), \
         patch("ingestion.sources.ccrs_client._write_checkpoint"):
        result = fetch_daily_update()
    assert result == 0


def test_fetch_daily_update_merges_with_existing_csv(tmp_path: Path) -> None:
    cp_file = tmp_path / ".cp.json"
    cp_file.write_text(json.dumps({"last_date": "2024-01-01"}))
    out_csv = tmp_path / "out.csv"
    pd.DataFrame([{"case_id": "EXIST001", "ROUTE": "80"}]).to_csv(out_csv, index=False)
    current_year = date.today().year
    fake_ids = {current_year: "some-resource-id"}

    fresh_df = pd.DataFrame([{"COLLISION_DATE": "2025-01-15", "case_id": "NEW001", "ROUTE": "80"}])

    with patch("ingestion.sources.ccrs_client.CHECKPOINT_FILE", cp_file), \
         patch("ingestion.sources.ccrs_client.OUTPUT_CSV", out_csv), \
         patch("ingestion.sources.ccrs_client.CCRS_RESOURCE_IDS", fake_ids), \
         patch("ingestion.sources.ccrs_client._get_download_url", return_value="http://x.com/f.csv"), \
         patch("ingestion.sources.ccrs_client._stream_and_filter", return_value=fresh_df), \
         patch("ingestion.sources.ccrs_client._write_checkpoint"):
        result = fetch_daily_update()
    combined = pd.read_csv(out_csv)
    assert len(combined) == 2


