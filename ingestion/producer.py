"""Kafka producer — polls all 5 data sources every POLL_INTERVAL_SECONDS and
emits one ScoredReading JSON message per corridor segment.

Live mode: hits real APIs, sends to Kafka.
Dry-run mode (DRY_RUN=true): loads from test fixtures, no API calls or Kafka.
Useful for local dev and CI without needing a running Kafka broker.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog
from dotenv import load_dotenv

# picks up .env so you don't need to export vars manually
load_dotenv()

log = structlog.get_logger()

CORRIDORS = [
    # I-80: Sacramento → Truckee
    {"segment_id": "SEG_01", "name": "Sacramento",       "route": "I-80",   "lat": 38.5816, "lon": -121.4944, "elevation_ft": 30},
    {"segment_id": "SEG_02", "name": "Roseville",        "route": "I-80",   "lat": 38.7521, "lon": -121.2880, "elevation_ft": 164},
    {"segment_id": "SEG_03", "name": "Auburn",           "route": "I-80",   "lat": 38.8966, "lon": -121.0769, "elevation_ft": 1255},
    {"segment_id": "SEG_04", "name": "Colfax",           "route": "I-80",   "lat": 39.1002, "lon": -120.9533, "elevation_ft": 2421},
    {"segment_id": "SEG_05", "name": "Emigrant_Gap",     "route": "I-80",   "lat": 39.2835, "lon": -120.6715, "elevation_ft": 5224},
    {"segment_id": "SEG_06", "name": "Donner_Summit",    "route": "I-80",   "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227},
    {"segment_id": "SEG_07", "name": "Truckee",          "route": "I-80",   "lat": 39.3280, "lon": -120.1833, "elevation_ft": 5817},
    # US-50: Sacramento → South Lake Tahoe
    {"segment_id": "SEG_08", "name": "Placerville",      "route": "US-50",  "lat": 38.7296, "lon": -120.7985, "elevation_ft": 1867},
    {"segment_id": "SEG_09", "name": "Echo_Summit",      "route": "US-50",  "lat": 38.8235, "lon": -120.0352, "elevation_ft": 7382},
    {"segment_id": "SEG_10", "name": "South_Lake_Tahoe", "route": "US-50",  "lat": 38.9399, "lon": -119.9772, "elevation_ft": 6237},
    # HWY-88: Jackson → Carson Pass
    {"segment_id": "SEG_11", "name": "Jackson",          "route": "HWY-88", "lat": 38.3490, "lon": -120.7752, "elevation_ft": 1200},
    {"segment_id": "SEG_12", "name": "Pioneer",          "route": "HWY-88", "lat": 38.4330, "lon": -120.5707, "elevation_ft": 3300},
    {"segment_id": "SEG_13", "name": "Carson_Spur",      "route": "HWY-88", "lat": 38.7054, "lon": -120.1024, "elevation_ft": 7990},
    {"segment_id": "SEG_14", "name": "Kirkwood",         "route": "HWY-88", "lat": 38.6868, "lon": -120.0657, "elevation_ft": 7800},
    {"segment_id": "SEG_15", "name": "Carson_Pass",      "route": "HWY-88", "lat": 38.6940, "lon": -119.9800, "elevation_ft": 8573},
]

# fixtures live next to tests/ — used in dry_run so no network needed
FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"

# module-level flag — signal handlers flip this to False for clean shutdown
_running = True


def _handle_signal(signum: int, frame: object) -> None:
    """Graceful shutdown on SIGTERM/SIGINT — lets current poll cycle finish.

    Args:
        signum: signal number received
        frame: current stack frame (unused, required by signal API)
    """
    global _running
    log.info("shutdown_signal_received", signum=signum)
    _running = False


def _load_fixture(name: str) -> dict:
    """Load a JSON fixture file by filename.

    Args:
        name: filename inside tests/fixtures/

    Returns:
        parsed dict from the JSON file
    """
    return json.loads((FIXTURES_DIR / name).read_text())


def _build_reading_dry_run(
    segment: dict,
    cc_data: list,
    rwis_data: list,
    seismic_events: list,
    nws_data: dict,
    openmeteo_data: dict,
) -> dict:
    """Score one segment from pre-fetched data lists, return message dict.

    All API calls already done upstream — this just finds nearest stations,
    merges readings, runs the scorer, and serializes to a flat dict for Kafka.
    Uses pythagorean distance (not haversine) — close enough for ~5-mile radius.

    Args:
        segment: corridor waypoint dict (segment_id, lat, lon, elevation_ft, etc.)
        cc_data: list of ChainControlStatus objects from CWWP2
        rwis_data: list of RWISReading objects from CWWP2
        seismic_events: list of SeismicEvent objects from USGS
        nws_data: dict mapping segment_id → NWSForecast (or None)
        openmeteo_data: dict mapping segment_id → OpenMeteoReading (or None)

    Returns:
        flat dict ready to JSON-serialize and push to Kafka
    """
    from ingestion.sources.cwwp2_client import ChainControlStatus, RWISReading
    from ingestion.sources.usgs_client import SeismicEvent, nearest_event_within_km
    from scoring.safety_score import SegmentReading, compute_score

    seg_id = segment["segment_id"]
    lat = segment["lat"]
    lon = segment["lon"]

    # walk all CC stations, keep the closest one to this segment
    chain_control: Optional[str] = None
    road_closed: Optional[bool] = None
    best_cc_dist = float("inf")
    for cc in cc_data:
        import math
        # radians so units are consistent — rough but fine for these distances
        dlat = math.radians(cc.latitude - lat)
        dlon = math.radians(cc.longitude - lon)
        dist = math.sqrt(dlat**2 + dlon**2)
        if dist < best_cc_dist:
            best_cc_dist = dist
            # "None" string from API means no chain control active
            chain_control = cc.cc_status if cc.cc_status != "None" else None

    # same nearest-neighbor scan for RWIS road-weather stations
    surface_temp_c: Optional[float] = None
    visibility_miles: Optional[float] = None
    wind_gust_mph: Optional[float] = None
    best_rwis_dist = float("inf")
    for rwis in rwis_data:
        import math
        dlat = math.radians(rwis.latitude - lat)
        dlon = math.radians(rwis.longitude - lon)
        dist = math.sqrt(dlat**2 + dlon**2)
        if dist < best_rwis_dist:
            best_rwis_dist = dist
            surface_temp_c = rwis.surface_temp_c
            visibility_miles = rwis.visibility_miles
            wind_gust_mph = rwis.wind_gust_mph

    # NWS fills in wind gusts if RWIS didn't have one
    nws = nws_data.get(seg_id)
    if nws and wind_gust_mph is None:
        wind_gust_mph = nws.wind_gust_mph

    # Open-Meteo: primary snowfall source; also fills visibility/wind if still missing
    om = openmeteo_data.get(seg_id)
    snowfall_rate: Optional[float] = None
    if om:
        snowfall_rate = om.snowfall_in_hr
        if visibility_miles is None:
            visibility_miles = om.visibility_miles
        if wind_gust_mph is None:
            wind_gust_mph = om.wind_gust_mph

    # seismic — only care about events within 80km (landslide/rockfall radius)
    seismic_mag: Optional[float] = None
    nearest = nearest_event_within_km(seismic_events, lat, lon, radius_km=80.0)
    if nearest:
        seismic_mag = nearest.magnitude

    # pack into dataclass so compute_score can validate inputs
    reading = SegmentReading(
        segment_id=seg_id,
        chain_control=chain_control,
        road_closed=road_closed,
        snowfall_rate_in_hr=snowfall_rate,
        visibility_miles=visibility_miles,
        wind_gust_mph=wind_gust_mph,
        surface_temp_c=surface_temp_c,
        seismic_mag=seismic_mag,
    )
    scored = compute_score(reading)

    # flat dict — schema must stay in sync with road_event.avsc and BigQuery raw_road_events
    return {
        "segment_id": seg_id,
        "segment_name": segment["name"],
        "route": segment.get("route", ""),
        "lat": lat,
        "lon": lon,
        "elevation_ft": segment["elevation_ft"],
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "score": scored.score,
        "band": scored.band,
        "active_penalties": scored.active_penalties,
        "chain_control": chain_control,
        "road_closed": road_closed,
        "snowfall_rate_in_hr": snowfall_rate,
        "visibility_miles": visibility_miles,
        "wind_gust_mph": wind_gust_mph,
        "surface_temp_c": surface_temp_c,
        "seismic_mag": seismic_mag,
    }


def poll_once(producer=None, dry_run: bool = False) -> list[dict]:
    """Fetch all sources, score each segment, emit to Kafka.

    All API calls happen first, then scoring — avoids partial state where
    some segments have fresh data and others have stale.

    Args:
        producer: confluent_kafka.Producer instance, or None (dry_run / test)
        dry_run: if True, loads from fixtures instead of hitting real APIs

    Returns:
        list of message dicts emitted (useful for test assertions without Kafka)
    """
    # lazy imports — keeps startup fast if this module is imported without running
    from ingestion.sources.cwwp2_client import (
        ChainControlStatus,
        RWISReading,
        fetch_chain_control,
        fetch_rwis,
    )
    from ingestion.sources.usgs_client import SeismicEvent, fetch_seismic_events
    from ingestion.sources.nws_client import NWSForecast, fetch_all_forecasts
    from ingestion.sources.openmeteo_client import OpenMeteoReading, fetch_current

    topic = os.getenv("KAFKA_TOPIC", "sierra.road.events")

    if dry_run:
        # fixtures avoid hammering APIs during dev/CI — same code path as live
        cc_fixture = _load_fixture("cwwp2_cc_sample.json")
        rwis_fixture = _load_fixture("cwwp2_rwis_sample.json")
        usgs_fixture = _load_fixture("usgs_sample.json")
        om_fixture = _load_fixture("openmeteo_sample.json")
        nws_fixture = _load_fixture("nws_forecast_sample.json")

        # -- parse CC fixture into dataclasses --
        from ingestion.sources.cwwp2_client import _safe_float

        cc_data: list[ChainControlStatus] = []
        for item in cc_fixture.get("data", []):
            loc = item["location"]
            status = item["status"]
            lat_v = _safe_float(loc.get("latitude"))
            lon_v = _safe_float(loc.get("longitude"))
            # skip entries with no coords — can't do proximity math on them
            if lat_v is None or lon_v is None:
                continue
            elev = _safe_float(loc.get("elevation"))
            cc_data.append(ChainControlStatus(
                location_name=loc.get("locationName", ""),
                route=loc.get("route", ""),
                latitude=lat_v,
                longitude=lon_v,
                elevation_ft=int(elev) if elev else None,
                cc_status=status.get("ccStatus", "None"),
                cc_description=status.get("ccStatusDescription", ""),
            ))

        # -- parse RWIS fixture using the shared parser (handles real NTCIP structure) --
        from ingestion.sources.cwwp2_client import _parse_rwis_payload
        rwis_data: list[RWISReading] = _parse_rwis_payload(rwis_fixture)

        # -- parse USGS fixture — filter to bounding box so we don't score CA quakes on NV segments --
        seismic_events: list[SeismicEvent] = []
        from ingestion.sources.usgs_client import LAT_MIN, LAT_MAX, LON_MIN, LON_MAX
        for feature in usgs_fixture.get("features", []):
            props = feature["properties"]
            coords = feature["geometry"]["coordinates"]
            # GeoJSON order is [lon, lat, depth]
            lon_e, lat_e, depth = float(coords[0]), float(coords[1]), float(coords[2])
            if not (LAT_MIN <= lat_e <= LAT_MAX and LON_MIN <= lon_e <= LON_MAX):
                continue
            seismic_events.append(SeismicEvent(
                event_id=feature.get("id", ""),
                magnitude=float(props["mag"]),
                place=props.get("place", ""),
                time_ms=int(props.get("time", 0)),
                latitude=lat_e,
                longitude=lon_e,
                depth_km=depth,
                mag_type=props.get("magType"),
            ))

        # -- NWS: single fixture fanned out to all segments in dry_run --
        from ingestion.sources.nws_client import NWS_GRIDS, _parse_speed
        nws_data: dict[str, Optional[NWSForecast]] = {}
        periods = nws_fixture.get("properties", {}).get("periods", [])
        for seg in CORRIDORS:
            seg_id = seg["segment_id"]
            if periods:
                p = periods[0]  # just the current/next period
                gust_raw = p.get("windGust")
                gust: Optional[float] = None
                # API returns either a string like "25 mph" or a dict with "value"
                if isinstance(gust_raw, str):
                    gust = _parse_speed(gust_raw)
                elif isinstance(gust_raw, dict):
                    gust = gust_raw.get("value")
                precip = p.get("probabilityOfPrecipitation", {})
                # use known grid if available, else placeholder (0,0) for fixture
                _grid = NWS_GRIDS.get(seg_id, ("REV", 0, 0))
                _, x, y = _grid
                nws_data[seg_id] = NWSForecast(
                    segment_id=seg_id,
                    grid_x=x,
                    grid_y=y,
                    start_time=p.get("startTime", ""),
                    temperature_f=float(p["temperature"]) if p.get("temperature") is not None else None,
                    wind_speed_mph=_parse_speed(p.get("windSpeed")),
                    wind_gust_mph=gust,
                    short_forecast=p.get("shortForecast", ""),
                    precip_probability=precip.get("value") if isinstance(precip, dict) else None,
                )

        # -- Open-Meteo: one fixture reading fanned out to every segment --
        from ingestion.sources.openmeteo_client import KMH_TO_MPH, CM_TO_IN, M_TO_MILES
        current = om_fixture.get("current", {})
        om_reading = OpenMeteoReading(
            latitude=om_fixture.get("latitude", 0.0),
            longitude=om_fixture.get("longitude", 0.0),
            timestamp=current.get("time", ""),
            temperature_c=current.get("temperature_2m"),
            snowfall_cm=current.get("snowfall"),
            # convert cm → inches at parse time so scorer never sees raw cm
            snowfall_in_hr=current["snowfall"] * CM_TO_IN if current.get("snowfall") is not None else None,
            snow_depth_m=current.get("snow_depth"),
            wind_speed_mph=current["wind_speed_10m"] * KMH_TO_MPH if current.get("wind_speed_10m") is not None else None,
            wind_gust_mph=current["wind_gusts_10m"] * KMH_TO_MPH if current.get("wind_gusts_10m") is not None else None,
            visibility_miles=current["visibility"] * M_TO_MILES if current.get("visibility") is not None else None,
        )
        # same reading for all segments — fixture doesn't vary by location
        openmeteo_data = {seg["segment_id"]: om_reading for seg in CORRIDORS}

    else:
        # live path — actual HTTP calls to all 4 external APIs
        cc_data = fetch_chain_control()
        rwis_data = fetch_rwis()
        seismic_events = fetch_seismic_events()
        nws_data = fetch_all_forecasts(CORRIDORS)
        # one request per segment for Open-Meteo since it's lat/lon specific
        openmeteo_data = {
            seg["segment_id"]: fetch_current(seg["lat"], seg["lon"])
            for seg in CORRIDORS
        }

    # score + emit — same code path regardless of live vs dry_run
    messages: list[dict] = []
    for segment in CORRIDORS:
        msg = _build_reading_dry_run(
            segment, cc_data, rwis_data, seismic_events, nws_data, openmeteo_data
        )
        messages.append(msg)
        if producer is not None:
            producer.produce(
                topic=topic,
                key=msg["segment_id"],
                value=json.dumps(msg).encode("utf-8"),
            )
            log.info(
                "message_produced",
                segment_id=msg["segment_id"],
                score=msg["score"],
                band=msg["band"],
            )

    # flush waits for all pending messages to be delivered before returning
    if producer is not None:
        producer.flush()

    return messages


def main() -> None:
    """Entry point — polling loop until SIGTERM/SIGINT.

    Reads DRY_RUN and POLL_INTERVAL_SECONDS from env,
    sets up Kafka producer if not dry_run, then loops forever.
    """
    # register both so Docker stop and ctrl-C both trigger clean shutdown
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

    producer = None
    if not dry_run:
        from confluent_kafka import Producer

        # confluent_kafka producer — async internally, flush() makes it synchronous
        producer = Producer(
            {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
        )

    log.info("producer_starting", dry_run=dry_run, poll_interval=poll_interval)
    while _running:
        try:
            poll_once(producer=producer, dry_run=dry_run)
        except Exception as exc:
            # log and continue — don't let one bad poll kill the process
            log.error("poll_error", error=str(exc))
        if _running:
            time.sleep(poll_interval)

    log.info("producer_stopped")


if __name__ == "__main__":
    main()
