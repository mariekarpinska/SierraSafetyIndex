"""Unit tests for cwwp2_client using mocked HTTP."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import responses as responses_lib

import json

import responses as responses_lib

from ingestion.sources.cwwp2_client import (
    CC_URL,
    CMS_URL,
    RWIS_URL,
    CMSMessage,
    ChainControlStatus,
    RWISReading,
    _safe_float,
    fetch_chain_control,
    fetch_cms,
    fetch_rwis,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


@responses_lib.activate
def test_chain_control_returns_statuses() -> None:
    """Fixture with 3 locations returns 3 ChainControlStatus objects."""
    fixture = json.loads((FIXTURES / "cwwp2_cc_sample.json").read_text())
    responses_lib.add(responses_lib.GET, CC_URL, json=fixture, status=200)

    results = fetch_chain_control()

    assert len(results) == 3
    assert all(isinstance(r, ChainControlStatus) for r in results)


@responses_lib.activate
def test_chain_control_r2_at_donner() -> None:
    """Donner Summit entry has R2 chain control status."""
    fixture = json.loads((FIXTURES / "cwwp2_cc_sample.json").read_text())
    responses_lib.add(responses_lib.GET, CC_URL, json=fixture, status=200)

    results = fetch_chain_control()
    donner = next((r for r in results if "Donner" in r.location_name), None)

    assert donner is not None
    assert donner.cc_status == "R2"
    assert donner.latitude == 39.3232
    assert donner.longitude == -120.3253


@responses_lib.activate
def test_chain_control_fields_populated() -> None:
    """All returned objects have non-empty location_name and route."""
    fixture = json.loads((FIXTURES / "cwwp2_cc_sample.json").read_text())
    responses_lib.add(responses_lib.GET, CC_URL, json=fixture, status=200)

    results = fetch_chain_control()

    for r in results:
        assert r.location_name != ""
        assert r.route != ""
        assert isinstance(r.latitude, float)
        assert isinstance(r.longitude, float)


@responses_lib.activate
def test_rwis_returns_readings() -> None:
    """Fixture with 2 stations returns 2 RWISReading objects."""
    fixture = json.loads((FIXTURES / "cwwp2_rwis_sample.json").read_text())
    responses_lib.add(responses_lib.GET, RWIS_URL, json=fixture, status=200)

    results = fetch_rwis()

    assert len(results) == 2
    assert all(isinstance(r, RWISReading) for r in results)


def test_safe_float_none_returns_none() -> None:
    assert _safe_float(None) is None


def test_safe_float_valid_string() -> None:
    assert _safe_float("3.14") == pytest.approx(3.14)


def test_safe_float_invalid_string_returns_none() -> None:
    assert _safe_float("N/A") is None
    assert _safe_float("bad_value") is None


@responses_lib.activate
def test_chain_control_skips_entry_with_missing_coords() -> None:
    """Entry with null latitude is skipped."""
    fixture = json.loads((FIXTURES / "cwwp2_cc_sample.json").read_text())
    fixture["data"].append({
        "location": {
            "locationName": "Bad Entry", "route": "80",
            "latitude": None, "longitude": None, "elevation": "1000",
        },
        "status": {"ccStatus": "None", "ccStatusDescription": ""},
    })
    responses_lib.add(responses_lib.GET, CC_URL, json=fixture, status=200)

    results = fetch_chain_control()

    assert len(results) == 3


@responses_lib.activate
def test_rwis_skips_entry_with_missing_coords() -> None:
    """RWIS entry with null latitude is skipped."""
    fixture = json.loads((FIXTURES / "cwwp2_rwis_sample.json").read_text())
    fixture["data"].append({
        "rwis": {
            "index": "999",
            "location": {
                "locationName": "Bad RWIS", "route": "80",
                "latitude": None, "longitude": None, "elevation": "1000",
            },
            "rwisData": {},
        }
    })
    responses_lib.add(responses_lib.GET, RWIS_URL, json=fixture, status=200)

    results = fetch_rwis()

    assert len(results) == 2


@responses_lib.activate
def test_fetch_cms_returns_messages() -> None:
    """fetch_cms returns a list of CMSMessage objects."""
    fixture = {
        "data": [{
            "signId": "CMS001",
            "location": {
                "locationName": "I-80 at Donner Summit",
                "route": "80",
                "latitude": "39.3232",
                "longitude": "-120.3253",
            },
            "status": {"cmsMessage": "CHAIN CONTROL AHEAD"},
        }]
    }
    responses_lib.add(responses_lib.GET, CMS_URL, json=fixture, status=200)

    results = fetch_cms()

    assert len(results) == 1
    assert isinstance(results[0], CMSMessage)
    assert results[0].sign_id == "CMS001"
    assert results[0].message == "CHAIN CONTROL AHEAD"


@responses_lib.activate
def test_fetch_cms_skips_missing_coords() -> None:
    fixture = {
        "data": [
            {
                "signId": "GOOD",
                "location": {"locationName": "Good Sign", "route": "80",
                             "latitude": "39.3232", "longitude": "-120.3253"},
                "status": {"cmsMessage": "ROAD CLOSED"},
            },
            {
                "cmsId": "BAD",
                "location": {"locationName": "No Coords", "route": "80",
                             "latitude": None, "longitude": None},
                "status": {"message": "TEST"},
            },
        ]
    }
    responses_lib.add(responses_lib.GET, CMS_URL, json=fixture, status=200)

    results = fetch_cms()

    assert len(results) == 1


@responses_lib.activate
def test_fetch_cms_fallback_message_field() -> None:
    """Falls back to 'message' field when 'cmsMessage' absent."""
    fixture = {
        "data": [{
            "cmsId": "CMS002",
            "location": {"locationName": "Sign", "route": "80",
                         "latitude": "39.3232", "longitude": "-120.3253"},
            "status": {"message": "FALLBACK MESSAGE"},
        }]
    }
    responses_lib.add(responses_lib.GET, CMS_URL, json=fixture, status=200)

    results = fetch_cms()

    assert results[0].message == "FALLBACK MESSAGE"


@responses_lib.activate
def test_rwis_donner_has_sensor_data() -> None:
    """Donner Summit RWIS reading has surface temp, visibility, and wind gust."""
    fixture = json.loads((FIXTURES / "cwwp2_rwis_sample.json").read_text())
    responses_lib.add(responses_lib.GET, RWIS_URL, json=fixture, status=200)

    results = fetch_rwis()
    donner = next((r for r in results if "Donner" in r.location_name), None)

    assert donner is not None
    assert donner.surface_temp_c is not None
    assert donner.visibility_miles is not None
    assert donner.wind_gust_mph is not None
    assert donner.surface_temp_c < 0
