"""Open-Meteo current weather client for corridor waypoints."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import requests
import structlog

log = structlog.get_logger()

OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"

CURRENT_VARIABLES = (
    "temperature_2m,snowfall,snow_depth,wind_speed_10m,wind_gusts_10m,visibility"
)

KMH_TO_MPH = 0.621371
CM_TO_IN = 0.393701
M_TO_MILES = 0.000621371


@dataclass
class OpenMeteoReading:
    """Open-Meteo current conditions for one waypoint."""

    latitude: float
    longitude: float
    timestamp: str
    temperature_c: Optional[float]
    snowfall_cm: Optional[float]
    snowfall_in_hr: Optional[float]
    snow_depth_m: Optional[float]
    wind_speed_mph: Optional[float]
    wind_gust_mph: Optional[float]
    visibility_miles: Optional[float]


def fetch_current(latitude: float, longitude: float) -> OpenMeteoReading:
    """Fetch current weather from Open-Meteo for a lat/lon coordinate."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": CURRENT_VARIABLES,
        "wind_speed_unit": "kmh",
    }
    resp = requests.get(OPENMETEO_URL, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    current = payload.get("current", {})

    snowfall_cm = current.get("snowfall")
    wind_kmh = current.get("wind_speed_10m")
    gust_kmh = current.get("wind_gusts_10m")
    vis_m = current.get("visibility")

    reading = OpenMeteoReading(
        latitude=payload.get("latitude", latitude),
        longitude=payload.get("longitude", longitude),
        timestamp=current.get("time", ""),
        temperature_c=current.get("temperature_2m"),
        snowfall_cm=snowfall_cm,
        snowfall_in_hr=snowfall_cm * CM_TO_IN if snowfall_cm is not None else None,
        snow_depth_m=current.get("snow_depth"),
        wind_speed_mph=wind_kmh * KMH_TO_MPH if wind_kmh is not None else None,
        wind_gust_mph=gust_kmh * KMH_TO_MPH if gust_kmh is not None else None,
        visibility_miles=vis_m * M_TO_MILES if vis_m is not None else None,
    )
    log.info("openmeteo_fetched", lat=latitude, lon=longitude)
    return reading
