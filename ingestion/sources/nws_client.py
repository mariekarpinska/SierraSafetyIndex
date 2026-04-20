"""NWS hourly forecast client for corridor waypoints."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import requests
import structlog

log = structlog.get_logger()

# Known-good hardcoded grids (Reno office = REV) — avoids a points-API call for I-80 segments
NWS_GRIDS: dict[str, tuple[str, int, int]] = {
    "SEG_03": ("REV", 65,  74),   # Auburn
    "SEG_05": ("REV", 88,  82),   # Emigrant Gap
    "SEG_06": ("REV", 98,  90),   # Donner Summit
    "SEG_07": ("REV", 100, 91),   # Truckee
}

NWS_POINTS_URL = "https://api.weather.gov/points/{lat},{lon}"
NWS_BASE = "https://api.weather.gov/gridpoints/{office}/{x},{y}/forecast/hourly"

# runtime cache: segment_id → (office, x, y) populated by _resolve_grid on first call
_grid_cache: dict[str, tuple[str, int, int]] = {}


def _resolve_grid(segment_id: str, lat: float, lon: float) -> Optional[tuple[str, int, int]]:
    """Return (office, x, y) for a segment, looking up via NWS points API if needed."""
    if segment_id in NWS_GRIDS:
        return NWS_GRIDS[segment_id]
    if segment_id in _grid_cache:
        return _grid_cache[segment_id]
    try:
        resp = requests.get(
            NWS_POINTS_URL.format(lat=round(lat, 4), lon=round(lon, 4)),
            timeout=10,
            headers={"User-Agent": "sierra-safety-index/1.0"},
        )
        resp.raise_for_status()
        props = resp.json().get("properties", {})
        office = props.get("gridId")
        x = props.get("gridX")
        y = props.get("gridY")
        if office and x is not None and y is not None:
            result = (office, int(x), int(y))
            _grid_cache[segment_id] = result
            log.info("nws_grid_resolved", segment_id=segment_id, office=office, x=x, y=y)
            return result
    except Exception as exc:
        log.warning("nws_grid_lookup_failed", segment_id=segment_id, error=str(exc))
    return None


@dataclass
class NWSForecast:
    """NWS hourly forecast for one waypoint."""

    segment_id: str
    grid_x: int
    grid_y: int
    start_time: str
    temperature_f: Optional[float]
    wind_speed_mph: Optional[float]
    wind_gust_mph: Optional[float]
    short_forecast: str
    precip_probability: Optional[float]


def _parse_speed(speed_str: Optional[str]) -> Optional[float]:
    """Parse '25 mph' → 25.0, None on failure."""
    if not speed_str:
        return None
    match = re.search(r"[\d.]+", speed_str)
    return float(match.group()) if match else None


def fetch_forecast(segment_id: str, lat: Optional[float] = None, lon: Optional[float] = None) -> Optional[NWSForecast]:
    """Fetch next-hour NWS forecast for a corridor segment.

    If lat/lon are provided, unknown segments are looked up via the NWS points API.
    Returns None if the grid cannot be resolved or the request fails.
    """
    grid = _resolve_grid(segment_id, lat or 0.0, lon or 0.0) if (lat and lon) else NWS_GRIDS.get(segment_id)
    if grid is None:
        return None
    office, x, y = grid
    url = NWS_BASE.format(office=office, x=x, y=y)
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "sierra-safety-index/1.0"})
        resp.raise_for_status()
    except Exception as exc:
        log.warning("nws_forecast_failed", segment_id=segment_id, error=str(exc))
        return None
    payload = resp.json()
    periods = payload.get("properties", {}).get("periods", [])
    if not periods:
        return None
    period = periods[0]
    wind_gust_raw = period.get("windGust")
    wind_gust: Optional[float] = None
    if isinstance(wind_gust_raw, str):
        wind_gust = _parse_speed(wind_gust_raw)
    elif isinstance(wind_gust_raw, dict):
        wind_gust = wind_gust_raw.get("value")

    precip = period.get("probabilityOfPrecipitation", {})
    precip_val: Optional[float] = None
    if isinstance(precip, dict):
        precip_val = precip.get("value")

    log.info("nws_forecast_fetched", segment_id=segment_id, office=office)
    return NWSForecast(
        segment_id=segment_id,
        grid_x=x,
        grid_y=y,
        start_time=period.get("startTime", ""),
        temperature_f=float(period["temperature"]) if period.get("temperature") is not None else None,
        wind_speed_mph=_parse_speed(period.get("windSpeed")),
        wind_gust_mph=wind_gust,
        short_forecast=period.get("shortForecast", ""),
        precip_probability=precip_val,
    )


def fetch_all_forecasts(segments: Optional[list[dict]] = None) -> dict[str, Optional[NWSForecast]]:
    """Fetch NWS forecasts for all corridor segments.

    Pass the CORRIDORS list to enable dynamic grid lookup for segments not in NWS_GRIDS.
    Falls back to the hardcoded NWS_GRIDS keys if segments is not provided.
    """
    if segments:
        return {
            seg["segment_id"]: fetch_forecast(seg["segment_id"], seg["lat"], seg["lon"])
            for seg in segments
        }
    return {seg_id: fetch_forecast(seg_id) for seg_id in NWS_GRIDS}
