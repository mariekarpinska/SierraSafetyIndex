"""Unit tests for openmeteo_client using mocked HTTP."""
from __future__ import annotations

import json
from pathlib import Path

import responses as responses_lib

from ingestion.sources.openmeteo_client import (
    OPENMETEO_URL,
    OpenMeteoReading,
    fetch_current,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"

LAT = 39.3232
LON = -120.3253


@responses_lib.activate
def test_fetch_current_returns_reading() -> None:
    """fetch_current returns an OpenMeteoReading dataclass."""
    fixture = json.loads((FIXTURES / "openmeteo_sample.json").read_text())
    responses_lib.add(responses_lib.GET, OPENMETEO_URL, json=fixture, status=200)

    result = fetch_current(LAT, LON)

    assert isinstance(result, OpenMeteoReading)


@responses_lib.activate
def test_fetch_current_wind_gust_converted() -> None:
    """Wind gust is converted from km/h to mph."""
    fixture = json.loads((FIXTURES / "openmeteo_sample.json").read_text())
    responses_lib.add(responses_lib.GET, OPENMETEO_URL, json=fixture, status=200)

    result = fetch_current(LAT, LON)

    # fixture: wind_gusts_10m = 88.5 km/h → ~54.97 mph
    assert result.wind_gust_mph is not None
    assert abs(result.wind_gust_mph - 88.5 * 0.621371) < 0.01


@responses_lib.activate
def test_fetch_current_snowfall_converted() -> None:
    """Snowfall is present in both cm and in/hr fields."""
    fixture = json.loads((FIXTURES / "openmeteo_sample.json").read_text())
    responses_lib.add(responses_lib.GET, OPENMETEO_URL, json=fixture, status=200)

    result = fetch_current(LAT, LON)

    assert result.snowfall_cm is not None
    assert result.snowfall_in_hr is not None
    # fixture: snowfall = 1.5 cm → ~0.59 in
    assert abs(result.snowfall_in_hr - 1.5 * 0.393701) < 0.01


@responses_lib.activate
def test_fetch_current_visibility_converted() -> None:
    """Visibility is converted from meters to miles."""
    fixture = json.loads((FIXTURES / "openmeteo_sample.json").read_text())
    responses_lib.add(responses_lib.GET, OPENMETEO_URL, json=fixture, status=200)

    result = fetch_current(LAT, LON)

    # fixture: visibility = 500m → ~0.31 miles
    assert result.visibility_miles is not None
    assert abs(result.visibility_miles - 500 * 0.000621371) < 0.001
