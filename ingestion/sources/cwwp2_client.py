"""Caltrans CWWP2 client — chain control, RWIS weather stations, CMS signs.

D3 = Caltrans District 3: Sacramento valley, Placer, El Dorado, Alpine counties
     (I-80, US-50, eastern HWY-88 incl. Kirkwood/Carson Pass)
D10 = Caltrans District 10: Amador, Calaveras counties
     (western HWY-88: Jackson, Pioneer)
All endpoints are unauthenticated JSON, updated every 1-5 min.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import requests
import structlog

log = structlog.get_logger()

# D3 covers I-80, US-50, and eastern HWY-88 (Carson Spur → Carson Pass)
CC_URL_D3   = "https://cwwp2.dot.ca.gov/data/d3/cc/ccStatusD03.json"
RWIS_URL_D3 = "https://cwwp2.dot.ca.gov/data/d3/rwis/rwisStatusD03.json"
CMS_URL_D3  = "https://cwwp2.dot.ca.gov/data/d3/cms/cmsStatusD03.json"

# D10 covers western HWY-88 (Jackson, Pioneer — Amador County)
CC_URL_D10   = "https://cwwp2.dot.ca.gov/data/d10/cc/ccStatusD10.json"
RWIS_URL_D10 = "https://cwwp2.dot.ca.gov/data/d10/rwis/rwisStatusD10.json"
CMS_URL_D10  = "https://cwwp2.dot.ca.gov/data/d10/cms/cmsStatusD10.json"

# kept for backwards-compat — callers that imported these names still work
CC_URL   = CC_URL_D3
RWIS_URL = RWIS_URL_D3
CMS_URL  = CMS_URL_D3


@dataclass
class ChainControlStatus:
    """Chain control status for one Caltrans check location."""

    location_name: str
    route: str
    latitude: float
    longitude: float
    elevation_ft: Optional[int]
    cc_status: str  # "None" | "R1" | "R2" | "R3"
    cc_description: str


@dataclass
class RWISReading:
    """Road Weather Information System station reading — physical sensors embedded in road."""

    station_id: str
    location_name: str
    route: str
    latitude: float
    longitude: float
    elevation_ft: Optional[int]
    air_temp_c: Optional[float]
    surface_temp_c: Optional[float]   # actual pavement temp, used for black-ice detection
    surface_status: Optional[str]
    visibility_miles: Optional[float]
    wind_speed_mph: Optional[float]
    wind_gust_mph: Optional[float]
    precip_type: Optional[str]
    precip_rate: Optional[float]


@dataclass
class CMSMessage:
    """Changeable Message Sign — variable message boards on the freeway."""

    sign_id: str
    location_name: str
    route: str
    latitude: float
    longitude: float
    message: str


def _safe_float(value: Optional[str]) -> Optional[float]:
    """Parse float from string, returns None on empty/invalid."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# NTCIP 1204 essSurfaceStatus integer → human-readable string
_SURFACE_STATUS_MAP = {
    "3": "DRY", "4": "TRACE_MOISTURE", "5": "WET",
    "6": "CHEMICALLY_WET", "7": "ICE_WARNING", "8": "ICE_WATCH",
    "9": "NEARLY_FROZEN", "10": "FROZEN", "11": "ABSORBING",
    "12": "DEW", "13": "FROST",
}

# NTCIP 1204 essPrecipSituation integer → category
_PRECIP_SITUATION_MAP = {
    "7": "SNOW", "8": "SNOW", "9": "SNOW",
    "10": "RAIN", "11": "RAIN", "12": "RAIN",
    "13": "FROZEN", "14": "FROZEN", "15": "FROZEN",
    "16": "MIXED", "17": "MIXED", "18": "MIXED",
}


def _ntcip_temp(value) -> Optional[float]:
    """NTCIP temperature: tenths of °C, 1001 = unknown."""
    v = _safe_float(value)
    if v is None or v == 1001:
        return None
    return v / 10.0


def _ntcip_wind_mph(value) -> Optional[float]:
    """NTCIP wind speed: tenths of m/s, 65535 = unknown → mph."""
    v = _safe_float(value)
    if v is None or v == 65535:
        return None
    return (v / 10.0) * 2.23694


def _ntcip_visibility_miles(value) -> Optional[float]:
    """NTCIP visibility: meters, 100001 = unknown → miles."""
    v = _safe_float(value)
    if v is None or v >= 100001:
        return None
    return v / 1609.344


def _parse_cc_payload(payload: dict) -> list[ChainControlStatus]:
    results: list[ChainControlStatus] = []
    for item in payload.get("data", []):
        loc = item.get("location", {})
        status = item.get("status", {})
        lat = _safe_float(loc.get("latitude"))
        lon = _safe_float(loc.get("longitude"))
        if lat is None or lon is None:
            continue
        elev_raw = _safe_float(loc.get("elevation"))
        results.append(
            ChainControlStatus(
                location_name=loc.get("locationName", ""),
                route=loc.get("route", ""),
                latitude=lat,
                longitude=lon,
                elevation_ft=int(elev_raw) if elev_raw is not None else None,
                cc_status=status.get("ccStatus", "None"),
                cc_description=status.get("ccStatusDescription", ""),
            )
        )
    return results


def fetch_chain_control() -> list[ChainControlStatus]:
    """Fetch chain control from D3 (I-80/US-50/eastern HWY-88) and D10 (western HWY-88).

    Returns all check locations regardless of status — caller filters
    to the nearest one per segment.
    """
    results: list[ChainControlStatus] = []
    for url, district in [(CC_URL_D3, "D3"), (CC_URL_D10, "D10")]:
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            batch = _parse_cc_payload(resp.json())
            results.extend(batch)
            log.info("chain_control_fetched", district=district, count=len(batch))
        except Exception as exc:
            log.warning("chain_control_fetch_failed", district=district, error=str(exc))
    return results


def _parse_rwis_payload(payload: dict) -> list[RWISReading]:
    results: list[RWISReading] = []
    for item in payload.get("data", []):
        rwis = item.get("rwis", {})
        loc = rwis.get("location", {})
        lat = _safe_float(loc.get("latitude"))
        lon = _safe_float(loc.get("longitude"))
        if lat is None or lon is None:
            continue

        elev_raw = _safe_float(loc.get("elevation"))
        rdata = rwis.get("rwisData", {})

        # Air temp from first sensor table entry (NTCIP tenths of °C)
        temp_table = rdata.get("temperatureData", {}).get("essTemperatureSensorTable", [])
        air_temp_raw = (
            temp_table[0].get("essTemperatureSensorEntry", {}).get("essAirTemperature")
            if temp_table else None
        )

        # Surface temp from first pavement sensor entry
        pave_table = rdata.get("pavementSensorData", {}).get("essPavementSensorTable", [])
        pave_entry = pave_table[0].get("essPavementSensorEntry", {}) if pave_table else {}

        # Precip type: use situation map; essPrecipYesNo==2 means no precipitation
        precip_data = rdata.get("humidityPrecipData", {})
        if precip_data.get("essPrecipYesNo") == "2":
            precip_type = None
        else:
            precip_type = _PRECIP_SITUATION_MAP.get(str(precip_data.get("essPrecipSituation", "")))

        results.append(
            RWISReading(
                station_id=str(rwis.get("index", "")),
                location_name=loc.get("locationName", ""),
                route=loc.get("route", ""),
                latitude=lat,
                longitude=lon,
                elevation_ft=int(elev_raw) if elev_raw is not None else None,
                air_temp_c=_ntcip_temp(air_temp_raw),
                surface_temp_c=_ntcip_temp(pave_entry.get("essSurfaceTemperature")),
                surface_status=_SURFACE_STATUS_MAP.get(str(pave_entry.get("essSurfaceStatus", ""))),
                visibility_miles=_ntcip_visibility_miles(
                    rdata.get("visibilityData", {}).get("essVisibility")
                ),
                wind_speed_mph=_ntcip_wind_mph(rdata.get("windData", {}).get("essAvgWindSpeed")),
                wind_gust_mph=_ntcip_wind_mph(rdata.get("windData", {}).get("essMaxWindGustSpeed")),
                precip_type=precip_type,
                precip_rate=_safe_float(precip_data.get("essPrecipRate")),
            )
        )
    return results


def fetch_rwis() -> list[RWISReading]:
    """Fetch RWIS sensor readings from D3 and D10 stations.

    surface_temp_c is the most useful field — air temp is always a few
    degrees warmer than the road when freezing conditions develop.
    """
    results: list[RWISReading] = []
    for url, district in [(RWIS_URL_D3, "D3"), (RWIS_URL_D10, "D10")]:
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            batch = _parse_rwis_payload(resp.json())
            results.extend(batch)
            log.info("rwis_fetched", district=district, count=len(batch))
        except Exception as exc:
            log.warning("rwis_fetch_failed", district=district, error=str(exc))
    return results


def fetch_cms() -> list[CMSMessage]:
    """Fetch CMS sign messages from D3 and D10.

    Not used in scoring yet, but useful for context — signs often say
    "CHAIN CONTROL AHEAD" or "ROAD CLOSED" before the CC status updates.
    """
    results: list[CMSMessage] = []
    for url, district in [(CMS_URL_D3, "D3"), (CMS_URL_D10, "D10")]:
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("data", []):
                loc = item.get("location", {})
                lat = _safe_float(loc.get("latitude"))
                lon = _safe_float(loc.get("longitude"))
                if lat is None or lon is None:
                    continue
                sign_status = item.get("status", {})
                message = sign_status.get("cmsMessage", sign_status.get("message", ""))
                results.append(
                    CMSMessage(
                        sign_id=str(item.get("signId", item.get("cmsId", ""))),
                        location_name=loc.get("locationName", ""),
                        route=loc.get("route", ""),
                        latitude=lat,
                        longitude=lon,
                        message=message,
                    )
                )
            log.info("cms_fetched", district=district)
        except Exception as exc:
            log.warning("cms_fetch_failed", district=district, error=str(exc))
    return results
