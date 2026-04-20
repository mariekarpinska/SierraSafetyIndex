"""Unit tests for streaming/spark_consumer.py — pure Python, no JVM required."""
from __future__ import annotations

import json


def test_schema_parsing() -> None:
    """Sample JSON string parses into a dict with correct fields."""
    from streaming.spark_consumer import parse_event_dict, EVENT_SCHEMA

    sample = {
        "segment_id": "SEG_06",
        "segment_name": "Donner_Summit",
        "lat": 39.3232,
        "lon": -120.3253,
        "elevation_ft": 7227,
        "timestamp_utc": "2024-01-15T07:00:00+00:00",
        "score": 55,
        "band": "ORANGE",
        "active_penalties": ["R2 chain control (-30)"],
        "chain_control": "R2",
        "road_closed": None,
        "snowfall_rate_in_hr": 0.5,
        "visibility_miles": 0.8,
        "wind_gust_mph": 35.0,
        "surface_temp_c": -2.0,
        "seismic_mag": None,
    }

    result = parse_event_dict(json.dumps(sample))

    assert result["segment_id"] == "SEG_06"
    assert result["score"] == 55
    assert result["band"] == "ORANGE"
    assert "event_timestamp" in result

    schema_fields = {f.name for f in EVENT_SCHEMA.fields}
    assert "segment_id" in schema_fields
    assert "score" in schema_fields
    assert "band" in schema_fields
    assert "timestamp_utc" in schema_fields


def test_window_aggregation() -> None:
    """5 messages for SEG_06 in same 15-min window → avg_score == 60.0."""
    from streaming.spark_consumer import batch_aggregate

    scores = [80, 60, 40, 70, 50]
    records = [
        {
            "segment_id": "SEG_06",
            "timestamp_utc": f"2024-01-15T07:0{i}:00+00:00",
            "score": s,
            "band": "ORANGE",
        }
        for i, s in enumerate(scores)
    ]

    results = batch_aggregate(records)

    assert len(results) == 1
    assert abs(results[0]["avg_score"] - 60.0) < 0.01
    assert results[0]["min_score"] == 40
    assert results[0]["max_score"] == 80
    assert results[0]["event_count"] == 5


def test_dominant_band() -> None:
    """[GREEN, YELLOW, YELLOW, ORANGE, YELLOW] → dominant_band == YELLOW."""
    from streaming.spark_consumer import _dominant_band

    bands = ["GREEN", "YELLOW", "YELLOW", "ORANGE", "YELLOW"]
    result = _dominant_band(bands)
    assert result == "YELLOW"
