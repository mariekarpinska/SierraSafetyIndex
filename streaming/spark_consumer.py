"""Spark Structured Streaming consumer — reads sierra.road.events Kafka topic
and writes to GCS + BigQuery.

Data flow: Kafka → Spark (foreachBatch) → GCS parquet → BigQuery raw_road_events

Using foreachBatch instead of a native GCS/BQ connector because:
  - pyspark 4.0 Kafka connector doesn't have a built-in GCS sink
  - foreachBatch lets us control schema, partitioning, and BQ load config exactly

Local fallback (USE_GCP=false or no creds): writes parquet to ./data/processed/raw_events/
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

log = structlog.get_logger()

# schema for JSON messages produced by ingestion/producer.py
# active_penalties is an array — stored separately from the BQ scalar columns
EVENT_SCHEMA = StructType(
    [
        StructField("segment_id",          StringType(),           True),
        StructField("segment_name",        StringType(),           True),
        StructField("route",               StringType(),           True),
        StructField("lat",                 DoubleType(),           True),
        StructField("lon",                 DoubleType(),           True),
        StructField("elevation_ft",        IntegerType(),          True),
        StructField("timestamp_utc",       StringType(),           True),
        StructField("score",               IntegerType(),          True),
        StructField("band",                StringType(),           True),
        StructField("active_penalties",    ArrayType(StringType()), True),
        StructField("chain_control",       StringType(),           True),
        StructField("road_closed",         BooleanType(),          True),
        StructField("snowfall_rate_in_hr", DoubleType(),           True),
        StructField("visibility_miles",    DoubleType(),           True),
        StructField("wind_gust_mph",       DoubleType(),           True),
        StructField("surface_temp_c",      DoubleType(),           True),
        StructField("seismic_mag",         DoubleType(),           True),
    ]
)

# (name, BQ type) for raw_road_events — excludes active_penalties (ARRAY, handled separately)
_BQ_SCHEMA_DEFS: list[tuple[str, str]] = [
    ("segment_id",          "STRING"),
    ("segment_name",        "STRING"),
    ("route",               "STRING"),
    ("lat",                 "FLOAT"),
    ("lon",                 "FLOAT"),
    ("elevation_ft",        "INTEGER"),
    ("event_timestamp",     "TIMESTAMP"),
    ("score",               "INTEGER"),
    ("band",                "STRING"),
    ("chain_control",       "STRING"),
    ("road_closed",         "INTEGER"),
    ("snowfall_rate_in_hr", "FLOAT"),
    ("visibility_miles",    "FLOAT"),
    ("wind_gust_mph",       "FLOAT"),
    ("surface_temp_c",      "INTEGER"),
    ("seismic_mag",         "FLOAT"),
]

BQ_COLUMNS = [name for name, _ in _BQ_SCHEMA_DEFS]


_CHECKPOINT_BASE_DIR = os.environ.get("SIERRA_CHECKPOINT_DIR") or str(
    Path(tempfile.gettempdir()) / "sierra-checkpoints"
)
# Convert to file:// URI so Hadoop doesn't parse a Windows drive letter
# (e.g. "C:") as a filesystem scheme. Works identically on macOS and Windows.
_CHECKPOINT_BASE_URI = Path(_CHECKPOINT_BASE_DIR).resolve().as_uri()


def get_spark() -> SparkSession:
    """Create or retrieve the active SparkSession.

    local[1] — single thread is fine for this throughput (7 segments × 1 msg/5min).
    """
    return (
        SparkSession.builder.appName("SierraSafetyStreaming")
        .master("local[1]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
        .config("spark.sql.streaming.checkpointLocation", _CHECKPOINT_BASE_URI)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.memory.fraction", "0.6")
        .getOrCreate()
    )


def parse_events(df: DataFrame, spark: Optional[SparkSession] = None) -> DataFrame:
    """Parse raw Kafka JSON value bytes → typed DataFrame.

    Two input shapes supported:
      - Kafka streaming DF (has 'value' bytes column) — production path
      - DF with 'json_str' string column — used in unit tests without a broker
    """
    if "value" in df.columns:
        parsed = df.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
        ).select("data.*")
    elif "json_str" in df.columns:
        parsed = df.select(
            F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data")
        ).select("data.*")
    else:
        parsed = df

    return parsed.withColumn(
        "event_timestamp", F.to_timestamp(F.col("timestamp_utc"))
    )


def compute_window_aggregations(df: DataFrame) -> DataFrame:
    """Compute 15-minute tumbling window aggregations per segment_id."""
    windowed = df.withWatermark("event_timestamp", "30 minutes").groupBy(
        F.window(F.col("event_timestamp"), "15 minutes"),
        F.col("segment_id"),
    ).agg(
        F.avg("score").alias("avg_score"),
        F.min("score").alias("min_score"),
        F.max("score").alias("max_score"),
        F.count("*").alias("event_count"),
        F.first("band").alias("dominant_band"),
    )
    return windowed.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "segment_id",
        "avg_score",
        "min_score",
        "max_score",
        "event_count",
        "dominant_band",
    )


def _dominant_band(bands: list[str]) -> str:
    """Return the most common band from a list (for batch testing)."""
    if not bands:
        return "GREEN"
    return Counter(bands).most_common(1)[0][0]


def _gcs_credentials_available() -> bool:
    """Return True only if GCS credentials file is set and exists."""
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    return bool(creds) and Path(creds).exists()


def _use_gcp() -> bool:
    """Return True if GCP integration is enabled (USE_GCP != false)."""
    return os.getenv("USE_GCP", "true").lower() != "false"


# ---------------------------------------------------------------------------
# foreachBatch writer — core of the GCS+BigQuery sink
# ---------------------------------------------------------------------------

def _make_batch_writer(
    gcs_bucket: str,
    project_id: str,
    dataset: str,
) -> object:
    """Returns a foreachBatch function closed over GCS/BQ config.

    The inner _write_batch runs once per 60s trigger window. On GCP it goes
    parquet → GCS → BQ load job (avoids streaming insert quota). Locally it
    just writes parquet so tests and local dev still work without credentials.
    """

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        import pandas as pd

        pdf = batch_df.toPandas()

        # Ensure event_timestamp is timezone-aware for BigQuery
        if "event_timestamp" in pdf.columns:
            pdf["event_timestamp"] = pd.to_datetime(pdf["event_timestamp"], utc=True)

        if _use_gcp() and gcs_bucket and _gcs_credentials_available():
            _write_to_gcs_and_bq(pdf, batch_id, gcs_bucket, project_id, dataset)
        else:
            if gcs_bucket:
                log.warning(
                    "running_in_local_only_mode_bigquery_disabled",
                    reason="GCS credentials not found or USE_GCP=false",
                )
            _write_locally(pdf, batch_id)

    return _write_batch


def _write_to_gcs_and_bq(
    pdf, batch_id: int, gcs_bucket: str, project_id: str, dataset: str
) -> None:
    """Upload batch parquet to GCS then load into BigQuery via load_table_from_uri."""
    import pandas as pd
    from google.cloud import bigquery, storage

    # Prepare parquet (only BQ-compatible scalar columns)
    available = [c for c in BQ_COLUMNS if c in pdf.columns]
    pdf_bq = pdf[available].copy()

    # Coerce types to match existing BQ table schema
    if "road_closed" in pdf_bq.columns:
        pdf_bq["road_closed"] = pdf_bq["road_closed"].fillna(False).infer_objects(copy=False).astype(int)
    if "surface_temp_c" in pdf_bq.columns:
        pdf_bq["surface_temp_c"] = pdf_bq["surface_temp_c"].round().astype("Int64")

    # Ensure event_timestamp is tz-aware
    if "event_timestamp" in pdf_bq.columns:
        pdf_bq["event_timestamp"] = pd.to_datetime(pdf_bq["event_timestamp"], utc=True)

    # Drop rows missing the required TIMESTAMP key
    pdf_bq = pdf_bq.dropna(subset=["event_timestamp"])
    if pdf_bq.empty:
        return

    date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    temp_file = Path(f"./data/tmp/batch_{batch_id}.parquet")
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    import pyarrow as pa
    _PA_TYPE = {
        "STRING": pa.string(),
        "FLOAT": pa.float64(),
        "INTEGER": pa.int64(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "BOOLEAN": pa.bool_(),
    }
    pa_schema = pa.schema([
        pa.field(name, _PA_TYPE[bq_type])
        for name, bq_type in _BQ_SCHEMA_DEFS
        if name in pdf_bq.columns
    ])
    pdf_bq.to_parquet(temp_file, index=False, engine="pyarrow", schema=pa_schema)

    # Upload to GCS
    gcs_key = f"raw_events/{date_str}/batch_{batch_id}.parquet"
    storage_client = storage.Client()
    bucket_obj = storage_client.bucket(gcs_bucket)
    blob = bucket_obj.blob(gcs_key)
    blob.upload_from_filename(str(temp_file))
    log.info("batch_uploaded_to_gcs", gcs_path=f"gs://{gcs_bucket}/{gcs_key}", rows=len(pdf_bq))

    # Load from GCS into BigQuery
    bq_client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.raw_road_events"
    bq_schema = [bigquery.SchemaField(n, t) for n, t in _BQ_SCHEMA_DEFS]
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=bq_schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_timestamp",
        ),
        clustering_fields=["segment_id"],
    )
    uri = f"gs://{gcs_bucket}/{gcs_key}"
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    log.info("batch_loaded_to_bigquery", table=table_ref, rows=len(pdf_bq))

    temp_file.unlink(missing_ok=True)


def _ensure_bq_table(project_id: str, dataset: str) -> None:
    """Create raw_road_events if it doesn't exist; warn if schema has drifted.

    Never auto-drops the table — that would destroy all history. Schema drift
    needs manual intervention via BQ console or `bq update`.
    """
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    bq_client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.raw_road_events"
    expected = {name: bq_type for name, bq_type in _BQ_SCHEMA_DEFS}
    bq_schema = [bigquery.SchemaField(n, t) for n, t in _BQ_SCHEMA_DEFS]

    try:
        table = bq_client.get_table(table_ref)
        existing = {f.name: f.field_type for f in table.schema}
        mismatches = {k: (existing.get(k), v) for k, v in expected.items() if existing.get(k) != v}
        if not mismatches:
            return
        log.warning(
            "bq_schema_mismatch_detected",
            table=table_ref,
            mismatches=mismatches,
            action="proceeding with existing schema",
        )
        return
    except NotFound:
        pass

    schema_obj = bigquery.Table(table_ref, schema=bq_schema)
    schema_obj.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="event_timestamp"
    )
    schema_obj.clustering_fields = ["segment_id"]
    bq_client.create_table(schema_obj)
    log.info("bq_table_created", table=table_ref)


def _write_locally(pdf, batch_id: int) -> None:
    """Local-only fallback — writes parquet when USE_GCP=false or no creds found.

    Not used in production. Mainly for local testing without a GCP project.
    """
    local_path = Path("./data/processed/raw_events")
    local_path.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = local_path / f"batch_{date_str}_{batch_id}.parquet"
    pdf.to_parquet(out_file, index=False, engine="pyarrow")
    log.warning(
        "running_in_local_only_mode_bigquery_disabled",
        file=str(out_file),
        rows=len(pdf),
    )


# ---------------------------------------------------------------------------
# Streaming entry point
# ---------------------------------------------------------------------------

def run_streaming() -> None:
    """Start the Spark Structured Streaming job."""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler("logs/consumer.log")
    logging.getLogger().addHandler(file_handler)

    try:
        _run_streaming_inner()
    except Exception:
        tb = traceback.format_exc()
        log.error("streaming_failed", traceback=tb)
        with open("logs/consumer.log", "a") as f:
            f.write("\n[FATAL] streaming_failed\n")
            f.write(tb)
        raise


def _run_streaming_inner() -> None:
    spark = get_spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic   = os.getenv("KAFKA_TOPIC", "sierra.road.events")
    gcs_bucket    = os.getenv("GCS_PROCESSED_BUCKET", "")
    project_id    = os.getenv("GCP_PROJECT_ID", "sierra-safety-index")
    dataset       = os.getenv("BIGQUERY_DATASET", "sierra_safety")
    debug_sink    = os.getenv("DEBUG_SINK", "").lower()

    log.info("streaming_starting", kafka=kafka_servers, topic=kafka_topic, gcs_bucket=gcs_bucket)

    if _use_gcp() and _gcs_credentials_available():
        _ensure_bq_table(project_id, dataset)

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = parse_events(raw_df, spark)

    checkpoint_path = f"{_CHECKPOINT_BASE_URI}/raw_events"

    if debug_sink == "console":
        agg_df = compute_window_aggregations(parsed_df)
        query = (
            agg_df.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime="60 seconds")
            .start()
        )
    else:
        # Use foreachBatch to write raw events → GCS → BigQuery
        batch_writer = _make_batch_writer(gcs_bucket, project_id, dataset)
        query = (
            parsed_df.writeStream
            .foreachBatch(batch_writer)
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="60 seconds")
            .start()
        )

    query.awaitTermination()


# ---------------------------------------------------------------------------
# Pure-Python helpers — used by unit tests, no Spark/JVM dependency
# ---------------------------------------------------------------------------

def parse_event_dict(json_str: str) -> dict:
    """Parse a single event JSON string → dict, normalising timestamp key."""
    record = json.loads(json_str)
    record["event_timestamp"] = record.get("timestamp_utc", "")
    return record


def batch_aggregate(records: list[dict]) -> list[dict]:
    """15-min tumbling window aggregation over a list of event dicts.

    Mirrors compute_window_aggregations() above but pure Python — used in
    test_spark_consumer.py to verify the aggregation logic without starting Spark.
    """
    buckets: dict[tuple, list[int]] = defaultdict(list)
    bands:   dict[tuple, list[str]] = defaultdict(list)

    for rec in records:
        ts_str = rec.get("timestamp_utc", "")
        try:
            ts = datetime.fromisoformat(ts_str)
        except ValueError:
            continue
        bucket_minute = (ts.hour * 60 + ts.minute) // 15 * 15
        key = (ts.date(), bucket_minute, rec["segment_id"])
        buckets[key].append(rec["score"])
        bands[key].append(rec["band"])

    results = []
    for key, scores in buckets.items():
        results.append(
            {
                "window_bucket":  key[:2],
                "segment_id":     key[2],
                "avg_score":      sum(scores) / len(scores),
                "min_score":      min(scores),
                "max_score":      max(scores),
                "event_count":    len(scores),
                "dominant_band":  Counter(bands[key]).most_common(1)[0][0],
            }
        )
    return results


if __name__ == "__main__":
    run_streaming()
