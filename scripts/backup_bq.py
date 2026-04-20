"""Backup BigQuery tables by copying them to timestamped names in the same dataset."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = os.getenv("BIGQUERY_DATASET", "sierra_safety")

TABLES_TO_BACKUP = [
    "raw_road_events",
    "safety_scores",
]


def backup_table(client: bigquery.Client, table_name: str, stamp: str) -> None:
    src  = f"{PROJECT_ID}.{DATASET}.{table_name}"
    dest = f"{PROJECT_ID}.{DATASET}.{table_name}_bak_{stamp}"

    try:
        client.get_table(src)
    except Exception:
        print(f"  SKIP {src} — table not found")
        return

    job = client.copy_table(src, dest)
    job.result()
    print(f"  OK   {src}  →  {dest}")


def main() -> None:
    stamp  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    client = bigquery.Client(project=PROJECT_ID)
    print(f"Backing up to timestamp: {stamp}")
    for table in TABLES_TO_BACKUP:
        backup_table(client, table, stamp)
    print("Done.")


if __name__ == "__main__":
    main()
