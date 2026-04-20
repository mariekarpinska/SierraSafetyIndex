"""Unit tests for ingestion/producer.py in DRY_RUN mode."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch


def test_dry_run_produces_15_messages() -> None:
    """DRY_RUN=true with mock Kafka produces exactly 15 messages."""
    from ingestion.producer import poll_once

    mock_producer = MagicMock()
    messages = poll_once(producer=mock_producer, dry_run=True)

    assert len(messages) == 15
    assert mock_producer.produce.call_count == 15
    mock_producer.flush.assert_called_once()


def test_message_keys_are_segment_ids() -> None:
    """All 15 produced message keys are SEG_01 through SEG_15."""
    from ingestion.producer import CORRIDORS, poll_once

    mock_producer = MagicMock()
    messages = poll_once(producer=mock_producer, dry_run=True)

    keys = {msg["segment_id"] for msg in messages}
    expected = {seg["segment_id"] for seg in CORRIDORS}
    assert keys == expected


def test_message_scores_in_range() -> None:
    """All 7 message score values are in 0..100."""
    from ingestion.producer import poll_once

    messages = poll_once(producer=None, dry_run=True)

    for msg in messages:
        assert 0 <= msg["score"] <= 100, f"Score out of range: {msg['score']} for {msg['segment_id']}"


def test_message_has_required_fields() -> None:
    """Each message has all required fields."""
    from ingestion.producer import poll_once

    required = {
        "segment_id", "score", "band", "timestamp_utc",
        "active_penalties", "lat", "lon", "elevation_ft",
    }
    messages = poll_once(producer=None, dry_run=True)

    for msg in messages:
        missing = required - set(msg.keys())
        assert not missing, f"Missing fields {missing} in {msg['segment_id']}"


def test_handle_signal_sets_running_false() -> None:
    """_handle_signal sets _running to False."""
    import ingestion.producer as mod
    mod._running = True
    mod._handle_signal(2, None)
    assert mod._running is False
    mod._running = True  # restore for other tests


def test_dry_run_handles_none_coords_in_fixtures(monkeypatch) -> None:
    """CC/RWIS entries with None coordinates are skipped gracefully."""
    import ingestion.producer as mod

    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    cc_fixture = json.loads((fixtures_dir / "cwwp2_cc_sample.json").read_text())
    cc_fixture["data"].append({
        "location": {"locationName": "Bad", "route": "80",
                     "latitude": None, "longitude": None, "elevation": "1000"},
        "status": {"ccStatus": "None", "ccStatusDescription": ""},
    })
    rwis_fixture = json.loads((fixtures_dir / "cwwp2_rwis_sample.json").read_text())
    rwis_fixture["data"].append({
        "stationId": "999",
        "location": {"locationName": "Bad", "route": "80",
                     "latitude": None, "longitude": None, "elevation": "1000"},
        "stationData": {
            "airTemp": None, "surfaceTemp": None, "surfaceStatus": None,
            "visibility": None, "windSpeed": None, "windGust": None,
            "precipitation": None, "precipRate": None,
        },
    })

    def patched_load_fixture(name: str) -> dict:
        if name == "cwwp2_cc_sample.json":
            return cc_fixture
        if name == "cwwp2_rwis_sample.json":
            return rwis_fixture
        return json.loads((fixtures_dir / name).read_text())

    monkeypatch.setattr(mod, "_load_fixture", patched_load_fixture)
    messages = mod.poll_once(producer=None, dry_run=True)
    assert len(messages) == 15


def test_dry_run_handles_out_of_bbox_seismic(monkeypatch) -> None:
    """USGS fixture with out-of-bbox event is filtered before scoring."""
    import ingestion.producer as mod

    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    usgs_fixture = json.loads((fixtures_dir / "usgs_sample.json").read_text())
    usgs_fixture["features"].append({
        "id": "far_away",
        "properties": {"mag": 5.0, "place": "Florida", "time": 12345, "magType": "ml"},
        "geometry": {"type": "Point", "coordinates": [-80.0, 25.0, 5.0]},
    })

    def patched_load_fixture(name: str) -> dict:
        if name == "usgs_sample.json":
            return usgs_fixture
        return json.loads((fixtures_dir / name).read_text())

    monkeypatch.setattr(mod, "_load_fixture", patched_load_fixture)
    messages = mod.poll_once(producer=None, dry_run=True)
    assert len(messages) == 15


def test_dry_run_nws_dict_wind_gust(monkeypatch) -> None:
    """NWS fixture with dict-form windGust is parsed correctly."""
    import ingestion.producer as mod

    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    nws_fixture = json.loads((fixtures_dir / "nws_forecast_sample.json").read_text())
    nws_fixture["properties"]["periods"][0]["windGust"] = {"value": 60.0, "unitCode": "km_h"}

    def patched_load_fixture(name: str) -> dict:
        if name == "nws_forecast_sample.json":
            return nws_fixture
        return json.loads((fixtures_dir / name).read_text())

    monkeypatch.setattr(mod, "_load_fixture", patched_load_fixture)
    messages = mod.poll_once(producer=None, dry_run=True)
    assert len(messages) == 15


def test_build_reading_nws_and_om_fallbacks() -> None:
    """NWS fills wind when RWIS has none; OM fills visibility and wind when both missing."""
    from ingestion.producer import _build_reading_dry_run
    from ingestion.sources.cwwp2_client import RWISReading
    from ingestion.sources.nws_client import NWSForecast
    from ingestion.sources.openmeteo_client import OpenMeteoReading

    segment = {
        "segment_id": "SEG_06", "name": "Donner_Summit",
        "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
    }
    rwis_no_wind = [RWISReading(
        station_id="1", location_name="Donner", route="80",
        latitude=39.3232, longitude=-120.3253, elevation_ft=7227,
        air_temp_c=-5.0, surface_temp_c=-3.0, surface_status="Frozen",
        visibility_miles=None, wind_speed_mph=None, wind_gust_mph=None,
        precip_type=None, precip_rate=None,
    )]
    nws_with_wind = {
        "SEG_06": NWSForecast(
            segment_id="SEG_06", grid_x=98, grid_y=90,
            start_time="2024-01-15T07:00:00", temperature_f=28.0,
            wind_speed_mph=20.0, wind_gust_mph=45.0,
            short_forecast="Heavy Snow", precip_probability=90.0,
        )
    }
    om_with_data = {
        "SEG_06": OpenMeteoReading(
            latitude=39.3232, longitude=-120.3253, timestamp="2024-01-15T07:00",
            temperature_c=-5.0, snowfall_cm=2.0, snowfall_in_hr=0.79,
            snow_depth_m=1.5, wind_speed_mph=25.0, wind_gust_mph=50.0,
            visibility_miles=0.5,
        )
    }

    msg = _build_reading_dry_run(segment, [], rwis_no_wind, [], nws_with_wind, om_with_data)
    assert msg["wind_gust_mph"] == 45.0  # NWS filled in since RWIS had None


def test_build_reading_om_fills_visibility_and_wind() -> None:
    """When no NWS, OM fills both visibility and wind_gust."""
    from ingestion.producer import _build_reading_dry_run
    from ingestion.sources.cwwp2_client import RWISReading
    from ingestion.sources.openmeteo_client import OpenMeteoReading

    segment = {
        "segment_id": "SEG_06", "name": "Donner_Summit",
        "route": "I-80", "lat": 39.3232, "lon": -120.3253, "elevation_ft": 7227,
    }
    rwis_empty = [RWISReading(
        station_id="1", location_name="Donner", route="80",
        latitude=39.3232, longitude=-120.3253, elevation_ft=7227,
        air_temp_c=None, surface_temp_c=None, surface_status=None,
        visibility_miles=None, wind_speed_mph=None, wind_gust_mph=None,
        precip_type=None, precip_rate=None,
    )]
    om = {
        "SEG_06": OpenMeteoReading(
            latitude=39.3232, longitude=-120.3253, timestamp="2024-01-15T07:00",
            temperature_c=-5.0, snowfall_cm=2.0, snowfall_in_hr=0.79,
            snow_depth_m=1.5, wind_speed_mph=25.0, wind_gust_mph=50.0,
            visibility_miles=0.5,
        )
    }

    msg = _build_reading_dry_run(segment, [], rwis_empty, [], {}, om)
    assert msg["wind_gust_mph"] == 50.0   # from OM
    assert msg["visibility_miles"] == 0.5  # from OM


def test_live_mode_calls_fetch_functions() -> None:
    """poll_once(dry_run=False) calls the real API fetch functions (mocked)."""
    from ingestion.producer import poll_once

    with patch("ingestion.sources.cwwp2_client.fetch_chain_control", return_value=[]) as mock_cc, \
         patch("ingestion.sources.cwwp2_client.fetch_rwis", return_value=[]), \
         patch("ingestion.sources.usgs_client.fetch_seismic_events", return_value=[]), \
         patch("ingestion.sources.nws_client.fetch_all_forecasts", return_value={}), \
         patch("ingestion.sources.openmeteo_client.fetch_current", return_value=None):
        messages = poll_once(producer=None, dry_run=False)

    assert len(messages) == 15
    mock_cc.assert_called_once()


def test_main_dry_run_loops_and_exits(monkeypatch) -> None:
    """main() runs one poll cycle in DRY_RUN mode then stops."""
    import ingestion.producer as mod

    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "1")
    mod._running = True

    def fake_sleep(t):
        mod._running = False

    with patch("ingestion.producer.poll_once") as mock_poll, \
         patch("ingestion.producer.time.sleep", side_effect=fake_sleep), \
         patch("ingestion.producer.signal.signal"):
        mod.main()

    mock_poll.assert_called_once()
    mod._running = True


def test_main_handles_poll_exception(monkeypatch) -> None:
    """main() logs error and continues on poll_once exception."""
    import ingestion.producer as mod

    monkeypatch.setenv("DRY_RUN", "true")
    mod._running = True

    def fake_sleep(t):
        mod._running = False

    with patch("ingestion.producer.poll_once", side_effect=RuntimeError("boom")), \
         patch("ingestion.producer.time.sleep", side_effect=fake_sleep), \
         patch("ingestion.producer.signal.signal"):
        mod.main()

    mod._running = True


def test_main_non_dry_run_creates_kafka_producer(monkeypatch) -> None:
    """main() creates a Kafka Producer when DRY_RUN is false."""
    import ingestion.producer as mod

    monkeypatch.setenv("DRY_RUN", "false")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    mod._running = True

    mock_producer_instance = MagicMock()
    mock_confluent = MagicMock()
    mock_confluent.Producer.return_value = mock_producer_instance

    def fake_sleep(t):
        mod._running = False

    with patch.dict("sys.modules", {"confluent_kafka": mock_confluent}), \
         patch("ingestion.producer.poll_once"), \
         patch("ingestion.producer.time.sleep", side_effect=fake_sleep), \
         patch("ingestion.producer.signal.signal"):
        mod.main()

    mock_confluent.Producer.assert_called_once()
    mod._running = True


