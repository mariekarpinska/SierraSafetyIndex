"""Unit tests for nws_client using mocked HTTP."""
from __future__ import annotations

import json
from pathlib import Path

import responses as responses_lib


from ingestion.sources.nws_client import (
    NWS_BASE,
    NWS_GRIDS,
    NWSForecast,
    _parse_speed,
    fetch_all_forecasts,
    fetch_forecast,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _donner_url() -> str:
    return NWS_BASE.format(office="REV", x=98, y=90)


@responses_lib.activate
def test_fetch_forecast_returns_nws_forecast() -> None:
    """Fetching SEG_06 returns an NWSForecast dataclass."""
    fixture = json.loads((FIXTURES / "nws_forecast_sample.json").read_text())
    responses_lib.add(responses_lib.GET, _donner_url(), json=fixture, status=200)

    result = fetch_forecast("SEG_06")

    assert isinstance(result, NWSForecast)
    assert result.segment_id == "SEG_06"


@responses_lib.activate
def test_forecast_has_wind_gust() -> None:
    """Forecast for SEG_06 has a wind_gust_mph value from fixture."""
    fixture = json.loads((FIXTURES / "nws_forecast_sample.json").read_text())
    responses_lib.add(responses_lib.GET, _donner_url(), json=fixture, status=200)

    result = fetch_forecast("SEG_06")

    assert result is not None
    assert result.wind_gust_mph is not None
    assert result.wind_gust_mph == 45.0


@responses_lib.activate
def test_forecast_short_forecast_field() -> None:
    """Forecast short_forecast field matches fixture value."""
    fixture = json.loads((FIXTURES / "nws_forecast_sample.json").read_text())
    responses_lib.add(responses_lib.GET, _donner_url(), json=fixture, status=200)

    result = fetch_forecast("SEG_06")

    assert result is not None
    assert result.short_forecast == "Heavy Snow"


def test_fetch_forecast_no_grid_returns_none() -> None:
    """Fetching a segment with no NWS grid returns None without HTTP calls."""
    result = fetch_forecast("SEG_01")  # Sacramento — no grid configured
    assert result is None


def test_parse_speed_valid_string() -> None:
    assert _parse_speed("25 mph") == 25.0


def test_parse_speed_none_input() -> None:
    assert _parse_speed(None) is None


def test_parse_speed_empty_string() -> None:
    assert _parse_speed("") is None


def test_parse_speed_no_digits_returns_none() -> None:
    assert _parse_speed("unknown") is None


@responses_lib.activate
def test_fetch_forecast_empty_periods_returns_none() -> None:
    """Empty periods list → returns None."""
    fixture = {"properties": {"periods": []}}
    responses_lib.add(responses_lib.GET, _donner_url(), json=fixture, status=200)

    result = fetch_forecast("SEG_06")

    assert result is None


@responses_lib.activate
def test_fetch_forecast_dict_wind_gust() -> None:
    """windGust as a dict with 'value' key is parsed correctly."""
    fixture = json.loads((FIXTURES / "nws_forecast_sample.json").read_text())
    fixture["properties"]["periods"][0]["windGust"] = {"value": 55.0, "unitCode": "wmoUnit:km_h-1"}
    responses_lib.add(responses_lib.GET, _donner_url(), json=fixture, status=200)

    result = fetch_forecast("SEG_06")

    assert result is not None
    assert result.wind_gust_mph == 55.0


@responses_lib.activate
def test_fetch_all_forecasts_returns_dict_keyed_by_segment() -> None:
    """fetch_all_forecasts returns one entry per NWS_GRIDS segment."""
    fixture = json.loads((FIXTURES / "nws_forecast_sample.json").read_text())
    for seg_id, (office, x, y) in NWS_GRIDS.items():
        url = NWS_BASE.format(office=office, x=x, y=y)
        responses_lib.add(responses_lib.GET, url, json=fixture, status=200)

    results = fetch_all_forecasts()

    assert set(results.keys()) == set(NWS_GRIDS.keys())
    assert all(isinstance(v, NWSForecast) for v in results.values())
