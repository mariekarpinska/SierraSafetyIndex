"""Unit tests for usgs_client using mocked HTTP."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import responses as responses_lib

import numpy as np

from ingestion.sources.usgs_client import (
    USGS_URL,
    SeismicEvent,
    _haversine_km_vec,
    fetch_seismic_events,
    nearest_event_within_km,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


@responses_lib.activate
def test_fetch_seismic_returns_events() -> None:
    """Fixture with 2 in-bbox events returns 2 SeismicEvent objects."""
    fixture = json.loads((FIXTURES / "usgs_sample.json").read_text())
    responses_lib.add(responses_lib.GET, USGS_URL, json=fixture, status=200)

    results = fetch_seismic_events()

    assert len(results) == 2
    assert all(isinstance(e, SeismicEvent) for e in results)


@responses_lib.activate
def test_fetch_seismic_m32_event_present() -> None:
    """M3.2 event near Truckee is in the results."""
    fixture = json.loads((FIXTURES / "usgs_sample.json").read_text())
    responses_lib.add(responses_lib.GET, USGS_URL, json=fixture, status=200)

    results = fetch_seismic_events()
    m32 = next((e for e in results if abs(e.magnitude - 3.2) < 0.01), None)

    assert m32 is not None
    assert m32.latitude == 39.3
    assert m32.longitude == -120.4


@responses_lib.activate
def test_nearest_event_within_80km() -> None:
    """M3.2 event at -120.4, 39.3 is within 80km of Donner Summit."""
    fixture = json.loads((FIXTURES / "usgs_sample.json").read_text())
    responses_lib.add(responses_lib.GET, USGS_URL, json=fixture, status=200)

    events = fetch_seismic_events()
    # Donner Summit: 39.3232, -120.3253
    nearest = nearest_event_within_km(events, 39.3232, -120.3253, radius_km=80.0)

    assert nearest is not None
    assert abs(nearest.magnitude - 3.2) < 0.01


def test_haversine_km_vec_returns_array() -> None:
    """Vectorized haversine matches scalar for single point."""
    from ingestion.sources.usgs_client import _haversine_km, _haversine_km_vec

    lat2 = np.array([39.3280])
    lon2 = np.array([-120.1833])
    vec_dist = _haversine_km_vec(39.3232, -120.3253, lat2, lon2)[0]
    scalar_dist = _haversine_km(39.3232, -120.3253, 39.3280, -120.1833)
    assert abs(vec_dist - scalar_dist) < 0.01


def test_haversine_km_vec_multiple_points() -> None:
    lats = np.array([39.3232, 38.5816])
    lons = np.array([-120.3253, -121.4944])
    dists = _haversine_km_vec(39.3232, -120.3253, lats, lons)
    assert dists[0] == pytest.approx(0.0, abs=0.01)
    assert dists[1] > 50.0


@responses_lib.activate
def test_fetch_seismic_skips_feature_with_short_coords() -> None:
    """Feature with only 2 coordinates (missing depth) is skipped."""
    fixture = json.loads((FIXTURES / "usgs_sample.json").read_text())
    fixture["features"].append({
        "id": "short_coords",
        "properties": {"mag": 2.0, "place": "Somewhere", "time": 12345, "magType": "ml"},
        "geometry": {"type": "Point", "coordinates": [-120.4, 39.3]},
    })
    responses_lib.add(responses_lib.GET, USGS_URL, json=fixture, status=200)

    results = fetch_seismic_events()

    assert len(results) == 2


@responses_lib.activate
def test_fetch_seismic_skips_out_of_bbox() -> None:
    """Feature outside Sierra bounding box is filtered out."""
    fixture = json.loads((FIXTURES / "usgs_sample.json").read_text())
    fixture["features"].append({
        "id": "florida",
        "properties": {"mag": 3.0, "place": "Florida", "time": 12345, "magType": "ml"},
        "geometry": {"type": "Point", "coordinates": [-80.0, 25.0, 5.0]},
    })
    responses_lib.add(responses_lib.GET, USGS_URL, json=fixture, status=200)

    results = fetch_seismic_events()

    assert len(results) == 2


@responses_lib.activate
def test_fetch_seismic_skips_null_magnitude() -> None:
    """Feature with mag=null is skipped."""
    fixture = json.loads((FIXTURES / "usgs_sample.json").read_text())
    fixture["features"].append({
        "id": "no_mag",
        "properties": {"mag": None, "place": "Near Truckee", "time": 12345, "magType": None},
        "geometry": {"type": "Point", "coordinates": [-120.4, 39.3, 5.0]},
    })
    responses_lib.add(responses_lib.GET, USGS_URL, json=fixture, status=200)

    results = fetch_seismic_events()

    assert len(results) == 2


@responses_lib.activate
def test_nearest_event_none_when_far() -> None:
    """No event returned when all events are outside the radius."""
    fixture = json.loads((FIXTURES / "usgs_sample.json").read_text())
    responses_lib.add(responses_lib.GET, USGS_URL, json=fixture, status=200)

    events = fetch_seismic_events()
    # Sacramento is far from Truckee event
    nearest = nearest_event_within_km(events, 38.5816, -121.4944, radius_km=5.0)

    assert nearest is None
