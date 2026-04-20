"""USGS earthquake feed client for the Sierra Nevada corridor."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np
import requests
import structlog

log = structlog.get_logger()

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

# Bounding box for Sierra Nevada corridor
LAT_MIN, LAT_MAX = 38.0, 40.5
LON_MIN, LON_MAX = -122.0, -119.0

EARTH_RADIUS_KM = 6371.0


@dataclass
class SeismicEvent:
    """A seismic event from the USGS feed."""

    event_id: str
    magnitude: float
    place: str
    time_ms: int
    latitude: float
    longitude: float
    depth_km: float
    mag_type: Optional[str]


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _haversine_km_vec(
    lat1: float, lon1: float, lat2: np.ndarray, lon2: np.ndarray
) -> np.ndarray:
    """Vectorized haversine: one fixed point vs an array of points."""
    lat1r = math.radians(lat1)
    lon1r = math.radians(lon1)
    lat2r = np.radians(lat2)
    lon2r = np.radians(lon2)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = np.sin(dlat / 2) ** 2 + math.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def fetch_seismic_events() -> list[SeismicEvent]:
    """Fetch recent earthquakes from USGS filtered to the Sierra corridor bbox."""
    resp = requests.get(USGS_URL, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    results: list[SeismicEvent] = []
    for feature in payload.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])
        if len(coords) < 3:
            continue
        lon, lat, depth = float(coords[0]), float(coords[1]), float(coords[2])
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            continue
        mag = props.get("mag")
        if mag is None:
            continue
        results.append(
            SeismicEvent(
                event_id=feature.get("id", ""),
                magnitude=float(mag),
                place=props.get("place", ""),
                time_ms=int(props.get("time", 0)),
                latitude=lat,
                longitude=lon,
                depth_km=depth,
                mag_type=props.get("magType"),
            )
        )
    log.info("seismic_events_fetched", count=len(results))
    return results


def nearest_event_within_km(
    events: list[SeismicEvent], lat: float, lon: float, radius_km: float = 80.0
) -> Optional[SeismicEvent]:
    """Return the highest-magnitude seismic event within radius_km of a point."""
    nearby = [e for e in events if _haversine_km(lat, lon, e.latitude, e.longitude) <= radius_km]
    return max(nearby, key=lambda e: e.magnitude) if nearby else None
