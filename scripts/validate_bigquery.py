"""Validate that BigQuery tables exist and contain the expected data.

Exit codes:
  0 — all checks passed
  1 — one or more checks failed

Usage:
    python scripts/validate_bigquery.py
    python scripts/validate_bigquery.py --verbose

Checks:
  1. raw_road_events        exists and has >= 7 rows
  2. stg_road_events        exists (dbt view)
  3. mart_safety_score_history  exists and has > 0 rows
  4. mart_segment_risk_profile  exists and has > 0 rows
  5. mart row count < raw row count (transformation happened)
  6. All expected segments present in raw_road_events
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT_ID", "sierra-safety-index")
DATASET = os.getenv("BIGQUERY_DATASET", "sierra_safety")

EXPECTED_SEGMENTS = {
    "SEG_01", "SEG_02", "SEG_03", "SEG_04",
    "SEG_05", "SEG_06", "SEG_07",
}

MIN_RAW_ROWS = 7


def _run(sql: str) -> "list[dict]":
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT)
    return [dict(row) for row in client.query(sql).result()]


def _check(label: str, ok: bool, detail: str = "", verbose: bool = False) -> bool:
    mark = "OK" if ok else "FAIL"
    msg = f"  [{mark}] {label}"
    if detail and (verbose or not ok):
        msg += f"  ({detail})"
    print(msg)
    return ok


def run(verbose: bool = False) -> int:
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not creds or not Path(creds).exists():
        print(f"ERROR: GOOGLE_APPLICATION_CREDENTIALS not found: {creds}")
        return 1

    print(f"Validating BigQuery: {PROJECT}.{DATASET}\n")
    failures = 0

    # 1. raw_road_events exists and has >= MIN_RAW_ROWS
    try:
        rows = _run(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.{DATASET}.raw_road_events`")
        raw_count = rows[0]["cnt"]
        ok = raw_count >= MIN_RAW_ROWS
        failures += 0 if _check("raw_road_events", ok, f"{raw_count} rows (need >= {MIN_RAW_ROWS})", verbose) else 1
    except Exception as exc:
        _check("raw_road_events", False, str(exc))
        failures += 1
        raw_count = 0

    # 2. stg_road_events view exists (dbt staging)
    try:
        rows = _run(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.{DATASET}.stg_road_events`")
        cnt = rows[0]["cnt"]
        ok = cnt >= 0  # just checks it exists
        failures += 0 if _check("stg_road_events (dbt staging view)", True, f"{cnt} rows", verbose) else 1
    except Exception as exc:
        _check("stg_road_events (dbt staging view)", False,
               "Not found — run: cd dbt_project && dbt run")
        failures += 1

    # 3. mart_safety_score_history exists and has > 0 rows
    try:
        rows = _run(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.{DATASET}.mart_safety_score_history`")
        mart_hist_count = rows[0]["cnt"]
        ok = mart_hist_count > 0
        failures += 0 if _check("mart_safety_score_history", ok, f"{mart_hist_count} rows", verbose) else 1
    except Exception as exc:
        _check("mart_safety_score_history", False,
               "Not found — run: cd dbt_project && dbt run")
        failures += 1
        mart_hist_count = 0

    # 4. mart_segment_risk_profile exists and has > 0 rows
    try:
        rows = _run(f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.{DATASET}.mart_segment_risk_profile`")
        mart_risk_count = rows[0]["cnt"]
        ok = mart_risk_count > 0
        failures += 0 if _check("mart_segment_risk_profile", ok, f"{mart_risk_count} rows", verbose) else 1
    except Exception as exc:
        _check("mart_segment_risk_profile", False,
               "Not found — run: cd dbt_project && dbt run")
        failures += 1
        mart_risk_count = 0

    # 5. mart row count < raw row count (dbt transformation happened)
    if raw_count > 0 and mart_hist_count > 0:
        ok = mart_hist_count < raw_count
        failures += 0 if _check(
            "mart rows < raw rows (transformation occurred)",
            ok,
            f"mart_history={mart_hist_count} vs raw={raw_count}",
            verbose,
        ) else 1

    # 6. Expected I-80 segments present
    try:
        rows = _run(f"SELECT DISTINCT segment_id FROM `{PROJECT}.{DATASET}.raw_road_events`")
        found_segs = {r["segment_id"] for r in rows}
        missing = EXPECTED_SEGMENTS - found_segs
        ok = len(missing) == 0
        detail = f"found {sorted(found_segs)}" if verbose else f"missing: {sorted(missing)}" if missing else f"{len(found_segs)} segments"
        failures += 0 if _check("all I-80 segments present", ok, detail, verbose) else 1
    except Exception as exc:
        _check("all I-80 segments present", False, str(exc))
        failures += 1

    # 7. Data freshness (latest timestamp)
    if verbose and raw_count > 0:
        try:
            rows = _run(f"SELECT MAX(event_timestamp) AS latest FROM `{PROJECT}.{DATASET}.raw_road_events`")
            print(f"\n  Latest event_timestamp: {rows[0]['latest']}")
        except Exception:
            pass

    print(f"\n{'All checks passed' if failures == 0 else f'{failures} check(s) failed'}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    sys.exit(run(verbose=args.verbose))
