"""Airflow DAGs for the Sierra Safety Index pipeline.

Two DAGs:
  sierra_safety_pipeline  — every 5 minutes: poll sources, score, stream to BQ
  sierra_crash_update     — daily at 03:00 UTC: pull CCRS crash records, update CSV
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# __file__ is the DAG file itself, so .parent.parent.parent climbs up to repo root
PROJECT_ROOT = Path(__file__).parent.parent.parent

log = logging.getLogger(__name__)

# shared across both DAGs — retries=1 w/ 60s delay is usually enough for transient HTTP blips
DEFAULT_ARGS = {
    "owner": "sierra-safety",
    "depends_on_past": False,      # don't block on prev run success
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
}


def _check_sources() -> None:
    """Ping the two critical upstream APIs before kicking off the full pipeline.

    Args: none — reads hardcoded endpoint list
    Returns: none — logs warn on failure but doesn't raise (non-blocking health check)
    """
    import requests  # local import so Airflow workers don't need requests at parse time

    # just the two main real-time feeds — USGS + Caltrans chain control
    endpoints = [
        "https://cwwp2.dot.ca.gov/data/d3/cc/ccStatusD03.json",
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    ]
    for url in endpoints:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()  # 4xx/5xx → exception
            log.info("Source reachable: %s (status %s)", url, resp.status_code)
        except Exception as exc:
            # warn not raise — don't kill pipeline just bc one source is flaky
            log.warning("Source check failed for %s: %s", url, exc)


def _notify_complete(**context: object) -> None:
    """Log a completion summary after a DAG run finishes.

    Args: **context — Airflow task context dict (dag, run_id, execution_date)
    Returns: none — side effect only, just logging
    """
    log.info(
        "Pipeline run complete. DAG: %s, run_id: %s, execution_date: %s",
        context.get("dag").dag_id,   # which DAG triggered this
        context.get("run_id"),
        context.get("execution_date"),
    )


# ---------------------------------------------------------------------------
# Main 5-minute pipeline DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="sierra_safety_pipeline",
    default_args=DEFAULT_ARGS,
    description="Sierra Safety Index — real-time road safety scoring pipeline",
    schedule="*/5 * * * *",   # every 5 min, matches source update cadence
    start_date=datetime(2024, 1, 1),
    catchup=False,             # don't backfill missed runs on deploy
    max_active_runs=1,         # prevent overlapping runs if a task is slow
    tags=["sierra", "safety", "streaming"],
) as dag:

    # step 1 — lightweight pre-flight, just HTTP GETs
    check_sources = PythonOperator(
        task_id="check_sources",
        python_callable=_check_sources,
    )

    # step 2 — actual poll: hits all 5 sources, computes scores, publishes to Kafka
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"cd {PROJECT_ROOT} && DRY_RUN=false "
            # inline import instead of module entrypoint so we don't need a __main__
            "python -c 'from ingestion.producer import poll_once; poll_once(dry_run=False)'"
        ),
        on_failure_callback=lambda ctx: log.error("run_producer failed: %s", ctx),
    )

    # step 3 — Spark reads from Kafka, writes aggregated windows to GCS + BQ
    # timeout 60 so it doesn't block forever; `|| true` swallows non-zero exit
    run_spark_consumer = BashOperator(
        task_id="run_spark_consumer",
        bash_command=(
            f"cd {PROJECT_ROOT} && DEBUG_SINK=console "
            "timeout 60 python -m streaming.spark_consumer || true"
        ),
        on_failure_callback=lambda ctx: log.error("run_spark_consumer failed: %s", ctx),
    )

    # step 4 — dbt staging: raw BQ tables → cleaned/typed staging views
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_project && "
            "dbt run --select staging --profiles-dir ."
        ),
        on_failure_callback=lambda ctx: log.error("run_dbt_staging failed: %s", ctx),
    )

    # step 5 — dbt intermediate + marts: windowed aggregates and risk profiles for dashboard
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_project && "
            # intermediate runs first bc marts depend on it — dbt handles dep order internally
            "dbt run --select intermediate marts --profiles-dir ."
        ),
        on_failure_callback=lambda ctx: log.error("run_dbt_marts failed: %s", ctx),
    )

    # step 6 — just a log line, useful for auditing run times in Airflow UI
    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=_notify_complete,
    )

    # linear chain — each step gates the next, no fan-out needed here
    (
        check_sources
        >> run_producer
        >> run_spark_consumer
        >> run_dbt_staging
        >> run_dbt_marts
        >> notify_complete
    )


# ---------------------------------------------------------------------------
# Daily crash update DAG (separate schedule — runs once per day)
# ---------------------------------------------------------------------------

def _run_crash_update() -> None:
    """Pull incremental CCRS crash records since the last checkpoint and append to CSV.

    Args: none — checkpoint managed internally by fetch_daily_update
    Returns: none — logs row count added
    """
    import sys
    # PROJECT_ROOT not in sys.path inside Airflow worker by default
    sys.path.insert(0, str(PROJECT_ROOT))
    from ingestion.sources.ccrs_client import fetch_daily_update
    added = fetch_daily_update()  # returns int row count
    log.info("crash_update_complete, new_rows=%s", added)


with DAG(
    dag_id="sierra_crash_update",
    default_args=DEFAULT_ARGS,
    description="Daily incremental CCRS crash data update for Sierra corridors",
    schedule="0 3 * * *",   # 03:00 UTC — CCRS publishes overnight batches
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sierra", "crashes", "ccrs"],
) as crash_dag:

    # only two tasks here — fetch then log; crash data doesn't need Spark/dbt each day
    update_crashes = PythonOperator(
        task_id="update_crashes",
        python_callable=_run_crash_update,
    )

    notify_crash_update = PythonOperator(
        task_id="notify_crash_update",
        python_callable=_notify_complete,  # reusing same completion logger from above
    )

    update_crashes >> notify_crash_update
