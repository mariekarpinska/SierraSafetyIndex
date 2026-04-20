"""End-to-end integration tests for the Sierra Safety Index pipeline.

Test groups:
  1. Dry-run producer test (no Kafka needed)
  2. Kafka round-trip test (requires Docker Kafka)
  3. BigQuery pipeline test (requires GCP credentials + populated tables)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GCP_PROJECT     = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET      = os.getenv("BIGQUERY_DATASET", "sierra_safety")
GCP_CREDS       = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")


def _kafka_available() -> bool:
    import socket
    host, port_str = KAFKA_BOOTSTRAP.split(":")
    try:
        with socket.create_connection((host, int(port_str)), timeout=2):
            return True
    except OSError:
        return False


def _gcp_available() -> bool:
    return bool(GCP_PROJECT) and bool(GCP_CREDS) and Path(GCP_CREDS).exists()


# ---------------------------------------------------------------------------
# Group 1: Dry-run producer (no external deps)
# ---------------------------------------------------------------------------

def test_dry_run_full_pipeline() -> None:
    """Run one poll cycle in DRY_RUN mode and assert schema + score constraints."""
    os.environ["DRY_RUN"] = "true"
    os.environ["DEBUG_SINK"] = "console"

    from ingestion.producer import poll_once

    messages = poll_once(dry_run=True, producer=None)

    # 15 segments: 7 I-80 + 3 US-50 + 5 HWY-88
    assert len(messages) == 15, f"Expected 15 messages, got {len(messages)}"

    required_fields = {
        "segment_id", "score", "band", "timestamp_utc",
        "active_penalties", "lat", "lon", "elevation_ft",
    }
    valid_bands = {"GREEN", "YELLOW", "ORANGE", "RED", "BLACK"}

    for msg in messages:
        missing = required_fields - msg.keys()
        assert not missing, f"Message missing fields: {missing}"
        assert 0 <= msg["score"] <= 100, f"Score out of range: {msg['score']}"
        assert msg["band"] in valid_bands, f"Invalid band: {msg['band']}"

    non_green = [m for m in messages if m["band"] != "GREEN"]
    assert non_green, "Expected at least one non-GREEN segment with winter fixtures"

    for msg in messages:
        if msg["score"] < 80:
            assert msg["active_penalties"], (
                f"Score {msg['score']} < 80 but active_penalties empty for {msg['segment_id']}"
            )


# ---------------------------------------------------------------------------
# Group 2: Kafka round-trip (requires Docker Kafka)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _kafka_available(), reason="Kafka not running")
def test_kafka_produce_and_consume() -> None:
    """Produce one cycle to Kafka and verify messages are consumable."""
    from confluent_kafka import Consumer, Producer

    topic = os.getenv("KAFKA_TOPIC", "sierra.road.events")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    from ingestion.producer import poll_once
    messages = poll_once(producer=producer, dry_run=True)
    producer.flush()

    assert len(messages) == 15

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          "test-e2e",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])

    consumed = []
    for _ in range(30):
        msg = consumer.poll(timeout=1.0)
        if msg and not msg.error():
            consumed.append(json.loads(msg.value()))
        if len(consumed) >= 15:
            break

    consumer.close()
    assert len(consumed) >= 15, f"Expected >= 15 Kafka messages, got {len(consumed)}"


# ---------------------------------------------------------------------------
# Group 3: BigQuery pipeline (requires GCP credentials + populated BQ tables)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _gcp_available(), reason="GCP credentials not available")
def test_bigquery_raw_table_has_data() -> None:
    """raw_road_events must have >= 15 rows."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    sql = f"SELECT COUNT(*) AS cnt FROM `{GCP_PROJECT}.{BQ_DATASET}.raw_road_events`"
    rows = list(client.query(sql).result())
    cnt = rows[0]["cnt"]
    assert cnt >= 15, (
        f"raw_road_events has {cnt} rows — expected >= 15. "
        "Run: python scripts/upload_to_bigquery.py"
    )


@pytest.mark.skipif(not _gcp_available(), reason="GCP credentials not available")
def test_bigquery_mart_history_has_data() -> None:
    """mart_safety_score_history must have > 0 rows (dbt ran successfully)."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    sql = f"SELECT COUNT(*) AS cnt FROM `{GCP_PROJECT}.{BQ_DATASET}.mart_safety_score_history`"
    try:
        rows = list(client.query(sql).result())
        cnt = rows[0]["cnt"]
        assert cnt > 0, (
            f"mart_safety_score_history has {cnt} rows — run: cd dbt_project && dbt run"
        )
    except Exception as exc:
        pytest.fail(
            f"mart_safety_score_history query failed: {exc}\n"
            "Run: cd dbt_project && dbt run"
        )


@pytest.mark.skipif(not _gcp_available(), reason="GCP credentials not available")
def test_bigquery_mart_risk_profile_has_data() -> None:
    """mart_segment_risk_profile must have > 0 rows."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    sql = f"SELECT COUNT(*) AS cnt FROM `{GCP_PROJECT}.{BQ_DATASET}.mart_segment_risk_profile`"
    try:
        rows = list(client.query(sql).result())
        cnt = rows[0]["cnt"]
        assert cnt > 0, "mart_segment_risk_profile is empty — run dbt"
    except Exception as exc:
        pytest.fail(f"mart_segment_risk_profile query failed: {exc}")


@pytest.mark.skipif(not _gcp_available(), reason="GCP credentials not available")
def test_bigquery_mart_rows_less_than_raw() -> None:
    """mart_safety_score_history row count must be less than raw_road_events (aggregation occurred)."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)

    raw_cnt = list(client.query(
        f"SELECT COUNT(*) AS cnt FROM `{GCP_PROJECT}.{BQ_DATASET}.raw_road_events`"
    ).result())[0]["cnt"]

    mart_cnt = list(client.query(
        f"SELECT COUNT(*) AS cnt FROM `{GCP_PROJECT}.{BQ_DATASET}.mart_safety_score_history`"
    ).result())[0]["cnt"]

    if raw_cnt > 0 and mart_cnt > 0:
        assert mart_cnt < raw_cnt, (
            f"mart ({mart_cnt}) should be smaller than raw ({raw_cnt}) — "
            "dbt aggregation should reduce row count"
        )


@pytest.mark.skipif(not _gcp_available(), reason="GCP credentials not available")
def test_bigquery_expected_segments_present() -> None:
    """All 15 segments (I-80 + US-50 + HWY-88) should be in raw_road_events."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    rows = list(client.query(
        f"SELECT DISTINCT segment_id FROM `{GCP_PROJECT}.{BQ_DATASET}.raw_road_events`"
    ).result())
    found = {r["segment_id"] for r in rows}
    expected = {f"SEG_{i:02d}" for i in range(1, 16)}
    missing = expected - found
    assert not missing, (
        f"Missing segments in raw_road_events: {sorted(missing)}\n"
        "Run: python scripts/upload_to_bigquery.py"
    )
