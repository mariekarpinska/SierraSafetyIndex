# Sierra Safety Index — Setup & Running Guide

## Prerequisites

| Tool            | Version    | Notes                                                                       |
| --------------- | ---------- | --------------------------------------------------------------------------- |
| Python          | 3.11       |                                                                             |
| Docker Desktop  | any recent | Runs Kafka + Zookeeper + Schema Registry                                    |
| GCP project     | —          | BigQuery + GCS APIs enabled; service-account JSON key                       |
| Terraform       | ≥ 1.5      | Provisions all GCP resources                                                |
| Java            | 17         | Required by Spark (`JAVA_HOME` must be set)                                 |
| Apache Airflow  | 3.x        | Orchestration; Python package installed in the venv. **Windows: use WSL 2** |
| Hadoop winutils | 3.x        | **Windows only** — see note below                                           |

### Installing Prerequisites

The Prerequisites table above lists _what_ you need; this table lists _how_ to install it on each platform.

| Tool           | macOS (Homebrew)                                                                                                  | Linux (Debian/Ubuntu, apt)                                                                                                                     | Windows (winget)                                                              |
| -------------- | ----------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Python 3.11    | `brew install python@3.11` (then use `python3.11` to create the venv if `python` resolves to a different version) | `sudo apt install python3.11 python3.11-venv` (older Ubuntu needs the [deadsnakes PPA](https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa)) | `winget install --id Python.Python.3.11 -e`                                   |
| Terraform      | `brew tap hashicorp/tap && brew install hashicorp/tap/terraform`                                                  | `sudo apt install terraform` (after adding [HashiCorp's apt repo](https://developer.hashicorp.com/terraform/install#linux))                    | `winget install --id Hashicorp.Terraform -e`                                  |
| Docker Desktop | `brew install --cask docker` then `open -a Docker` to start the daemon                                            | `curl -fsSL https://get.docker.com \| sh` (installs Docker Engine + Compose plugin; Docker Desktop on Linux is optional)                       | `winget install --id Docker.DockerDesktop -e` then launch Docker Desktop once |
| Java 17        | `brew install --cask temurin@17` (skip if `java -version` already reports 17)                                     | `sudo apt install openjdk-17-jdk`                                                                                                              | `winget install --id EclipseAdoptium.Temurin.17.JDK -e`                       |

> **Note:** The macOS and Windows commands above have been tested end-to-end. The Linux column is provided for reference only — it has not been verified on a real install.

After installing, confirm versions:

```bash
python3.11 --version            # 3.11.x
terraform -version              # ≥ 1.5
docker compose version          # confirms Compose V2 is on PATH
java -version                   # 17.x.x
```

#### Windows: post-install setup

Java needs `JAVA_HOME` set, and Spark on Windows additionally requires Hadoop winutils:

```powershell
# 1. Set JAVA_HOME (adjust the path to match your Temurin install):
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-17.x.x", "User")
[System.Environment]::SetEnvironmentVariable("PATH", "$env:PATH;$env:JAVA_HOME\bin", "User")

# 2. Download Hadoop winutils for Hadoop 3.x:
#    https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin
# 3. Place winutils.exe in C:\hadoop\bin\
# 4. Set HADOOP_HOME:
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[System.Environment]::SetEnvironmentVariable("PATH", "$env:PATH;C:\hadoop\bin", "User")
```

For a fuller walkthrough of installing Hadoop on Windows, see [this gist](https://gist.github.com/vorpal56/5e2b67b6be3a827b85ac82a63a5b3b2e).

---

## GCP Project Setup

Before running any setup steps, you need a GCP project with billing enabled and a service-account key on disk. If you already have one, skip to the next section.

1. **Create a project** at [console.cloud.google.com](https://console.cloud.google.com/projectcreate) and note the **Project ID** (not the display name).
2. **Enable billing** on the project (required for BigQuery and GCS, even within the free tier).
3. **Enable the APIs** used by this pipeline:
   - [BigQuery API](https://console.cloud.google.com/apis/library/bigquery.googleapis.com)
   - [Cloud Storage API](https://console.cloud.google.com/apis/library/storage.googleapis.com)
   - [Cloud Resource Manager API](https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com) (required by Terraform)
4. **Create a service account** at _IAM & Admin → Service Accounts → Create Service Account_. Grant it these roles:
   - `BigQuery Admin`
   - `Storage Admin`
   - `Service Account User`
5. **Create a JSON key** for that service account (_Keys → Add Key → JSON_) and save it somewhere outside the repo, e.g. `~/.gcp/sierra-safety-sa.json`. **Do not commit it.**
6. **Populate `.env`** with the three required values:

   ```
   GCP_PROJECT_ID=your-project-id
   GCS_PROCESSED_BUCKET=sierra-safety-processed-your-project-id
   GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/sierra-safety-sa.json
   ```

   The bucket name must be globally unique — suffixing with the project ID is a safe default. Terraform will create the bucket; the value here just needs to match what Terraform provisions.

---

## Step-by-Step Setup

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd sierra-safety-index

# 2. Copy and populate the environment file (see "GCP Project Setup" above)
cp .env.example .env
# Edit .env — set GCP_PROJECT_ID, GCS_PROCESSED_BUCKET, GOOGLE_APPLICATION_CREDENTIALS

# 3. Create and activate a virtual environment
# First, verify your `python` is 3.11:
python --version        # should print: Python 3.11.x
# If it prints a different version (common on macOS with pyenv/Homebrew),
# use an explicit 3.11 interpreter for the next command:
#   macOS:   python3.11 -m venv .venv
#   Windows: py -3.11 -m venv .venv
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Provision GCP infrastructure — first load .env into the shell so
#    terraform can see GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, etc.

# Windows (PowerShell):
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
    [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
  }
}
# Skip this block if you're using make.ps1 — its 'init' step loads .env for you.

# macOS / Linux (bash or zsh):
set -a; source .env; set +a

cd terraform
terraform init
terraform apply
cd ..
# Note: If terraform apply fails with "Cloud Resource Manager API not enabled",
# enable it at https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com
# then re-run.

# 6. Start Kafka
make kafka-up
# On Windows without make: .\make.ps1 kafka-up
# make.ps1 is the tested Windows equivalent of the Makefile — same targets, native PowerShell

# 7. (Optional) Generate and upload historical backfill data to BigQuery.
#    A fresh clone has no backfill parquet files — the repo does not ship them.
#    Skip this step if you're happy to let the live producer (step 9) accumulate
#    data over time; the dashboard will just have less history until it catches up.
#    To seed 180 days of history for all 15 segments now:
python -m ingestion.backfill            # generates data/processed/raw_events/*.parquet
python scripts/upload_to_bigquery.py    # uploads that backfill to BigQuery

# 8. Optionally download + upload crash data
python -m ingestion.sources.ccrs_client --years 2024 2025 2026
python scripts/upload_to_bigquery.py --crashes

# 9. Run dbt to materialize mart tables

# First, install the dbt profile so dbt can find BigQuery credentials.
# `dbt_project/profiles.yml.example` references env vars from .env,
# so the same file works on macOS, Linux, and Windows.
# macOS / Linux:
mkdir -p ~/.dbt && cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml
# Windows (PowerShell):
# New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.dbt" | Out-Null
# Copy-Item dbt_project\profiles.yml.example "$env:USERPROFILE\.dbt\profiles.yml"

cd dbt_project
dbt debug           # verify BigQuery connection
dbt run             # materialize all models
cd ..

# 10. Run the live producer (polls all sources every 5 minutes)
python -m ingestion.producer

# 11. In a second terminal, start the Spark consumer (streams Kafka → GCS → BigQuery).
#     A fresh terminal does NOT inherit the venv or the env vars from step 5 —
#     re-activate and re-load them before running the consumer:
#       macOS / Linux:  source .venv/bin/activate && set -a && source .env && set +a
#       Windows (PS):   .venv\Scripts\activate       # then re-run the .env loader block from step 5
python -m streaming.spark_consumer

# 12. Launch the dashboard
streamlit run dashboard.py
```

---

## Running Without GCP (Local Mode)

Set `USE_GCP=false` in `.env`. The Spark consumer will write to `data/processed/raw_events/` instead of BigQuery. The dashboard will show an error (by design — local files are not a valid data source in production). Use this mode for local development and testing only.

```bash
USE_GCP=false python -m streaming.spark_consumer
```

---

## Backfill

To generate historical scored readings for all segments:

```bash
# Last 180 days, all 15 segments
python -m ingestion.backfill

# Specific date range
python -m ingestion.backfill --start 2025-10-01 --end 2026-04-01

# Include crash penalties (requires data/switrs/switrs_crashes.csv)
# First, download crash data from data.ca.gov (CCRS — California Crash Reporting System):
python -m ingestion.sources.ccrs_client            # last 2 calendar years (recommended)
python -m ingestion.sources.ccrs_client --years 2024 2025  # specific years
# Downloads statewide CSVs (~34 MB each), filters to I-80/US-50/HWY-88,
# and writes to data/switrs/switrs_crashes.csv automatically.

# Then run the backfill with crash penalties:
python -m ingestion.backfill --with-crashes

# After backfill, upload to BigQuery
python scripts/upload_to_bigquery.py
```

---

## Orchestration (Airflow)

Airflow handles two schedules automatically instead of running things manually:

- `sierra_safety_pipeline` — every 5 min: producer → Spark consumer → dbt
- `sierra_crash_update` — daily at 03:00 UTC: incremental CCRS crash download

**One-time setup** (inside the venv, after installing Airflow per the Prerequisites table):

```bash
# macOS / Linux (and Windows via WSL 2) — point Airflow at this project's dags folder
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False   # hide the bundled tutorial DAGs
# add these to ~/.bashrc or ~/.zshrc to avoid re-exporting in every new terminal
```

```powershell
# Windows (native PowerShell) — Airflow does not officially run here; use WSL 2.
$env:AIRFLOW_HOME = "$env:USERPROFILE\airflow"
$env:AIRFLOW__CORE__DAGS_FOLDER = "$PWD\airflow\dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
```

**Start Airflow (simplest path — Airflow 3.x):**

```bash
airflow standalone
```

`airflow standalone` runs the metadata DB migration, creates an `admin` user, and starts the scheduler, triggerer, DAG processor, and API server as a single foreground process. The UI is at http://localhost:8080. Ctrl-C stops everything.

**Finding the admin login:** the generated password is printed to stdout at startup, but the log is long — if you missed it, open a second terminal and read the credentials file:

```bash
# macOS / Linux (and Windows via WSL 2)
cat ~/airflow/simple_auth_manager_passwords.json.generated

# Windows (native PowerShell — Airflow itself is not supported here; use WSL 2)
Get-Content "$env:USERPROFILE\airflow\simple_auth_manager_passwords.json.generated"
```

**Start Airflow (separate processes — for production-style runs):**

```bash
# terminal 1 — required, triggers DAG runs on schedule
airflow scheduler

# terminal 2 — required in 3.x, serves the UI + REST API on :8080
airflow api-server

# terminal 3 — required in 3.x, parses DAG files and pushes them to the scheduler
airflow dag-processor
```

**Note:** Airflow does not officially support Windows. Run inside WSL 2 or skip Airflow and run the producer/consumer manually (see Step-by-Step Setup above). The crash update can be triggered manually at any time:

```bash
python -m ingestion.sources.ccrs_client --daily
```

---

## Validation

```bash
# Check BigQuery tables have data and dbt models are materialized
python scripts/validate_bigquery.py --verbose
```

Exit code 0 = all checks passed. Checks:

- `raw_road_events` has ≥ 7 rows
- `stg_road_events` (dbt view) exists
- `mart_safety_score_history` has > 0 rows
- `mart_segment_risk_profile` has > 0 rows
- mart row count < raw row count (transformation occurred)
- All 7 I-80 segments present

---

## Running Tests

```bash
# Unit tests only (no Docker or GCP required — 36 tests)
pytest tests/unit/ -v

# Integration tests
# - Dry-run producer tests run without any external deps
# - Kafka tests are skipped if Docker Kafka is not running
# - BigQuery tests are skipped if GCP credentials are not set
pytest tests/integration/ -v
```
