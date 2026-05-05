# Sierra Safety Index

> ⚠️ **Disclaimer**: This is a research and educational tool. It is NOT driving advice. Always check Caltrans QuickMap and use your own judgment before driving in mountain conditions.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?logo=googlebigquery&logoColor=white)
![GCS](https://img.shields.io/badge/Google_Cloud_Storage-4285F4?logo=googlecloud&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?logo=terraform&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
[![codecov](https://codecov.io/gh/mariekarpinska/SierraSafetyIndex/branch/main/graph/badge.svg)](https://codecov.io/gh/mariekarpinska/SierraSafetyIndex)

---

## Problem Description

### The Problem

Every winter, tens of thousands of drivers cross the Sierra Nevada mountain corridors (I-80, US-50, HWY-88) through rapidly changing and potentially life-threatening road conditions. Snow storms, ice, high winds, chain control requirements, and seismic activity can transform a safe highway into an extreme hazard within minutes.

**This free public tool answers the question: "How dangerous is this road right now, and what does history say about conditions like these?"**

Individual data sources exist in silos:

- Caltrans publishes chain control status — but not combined with weather
- RWIS road-weather sensors report visibility and surface temperature — but not combined with crash history
- NWS forecasts snowfall — but not cross-referenced with past accident rates
- CCRS tracks crash records — but not linked to the concurrent road conditions

Drivers are left to manually check 4–5 separate websites and mentally synthesize the risk themselves. For inexperienced drivers — or anyone unfamiliar with mountain driving — this is error-prone and often skipped entirely.

### The Solution

The **Sierra Safety Index** is a real-time streaming data pipeline that fuses six free public data sources every five minutes to compute a per-segment **Drive Safety Score** (0–100) for 15 waypoints across three Sierra Nevada corridors.

**What it does:**

- Ingests Caltrans chain control status, RWIS road-weather sensor readings, NWS hourly forecasts, Open-Meteo snowfall/wind data, USGS seismic events, and CCRS crash records every 5 minutes via Kafka
- Applies a calibrated weighted penalty formula to produce a single 0–100 safety score per segment
- Streams data through Spark Structured Streaming → GCS → BigQuery, building a queryable historical time-series
- Transforms raw data through a dbt pipeline into dashboard-ready mart tables
- Surfaces results in a three-tile Streamlit dashboard showing current conditions with historical crash context, safety score trends over time, and a crash hotspot map

---

## Architecture

![Architecture Diagram](docs/images/Architecture.PNG)

The end-to-end data flow is:

```
Public APIs → Kafka (producer) → Spark Structured Streaming (consumer) → GCS → BigQuery → dbt → Streamlit
```
### Cloud & IaC (Terraform)

Infrastructure runs on **Google Cloud Platform (us-central1)**. BigQuery datasets and tables, GCS buckets (raw + processed zones), and IAM bindings are declared in `terraform/main.tf` and provisioned with `terraform apply`, so the environment can be recreated from code rather than set up by hand in the console.

**Resources provisioned by Terraform:**

- GCS bucket: `sierra-safety-raw-{project_id}` (raw Kafka payloads)
- GCS bucket: `sierra-safety-processed-{project_id}` (Spark output, Parquet)
- BigQuery dataset: `sierra_safety`
- BigQuery tables: `raw_road_events` (partitioned + clustered), `safety_scores`, `crashes` (partitioned + clustered)
- IAM: service account bindings for BigQuery and GCS read/write

---

## Tech Stack

| Layer            | Tool                               |
| ---------------- | ---------------------------------- |
| Cloud            | GCP                                |
| IaC              | Terraform                          |
| Orchestration    | Apache Airflow                     |
| Message broker   | Apache Kafka (Docker)              |
| Stream processor | Spark Structured Streaming         |
| Data lake        | GCS (raw + processed zones)        |
| Data warehouse   | BigQuery (partitioned + clustered) |
| Transforms       | dbt (bigquery adapter)             |
| Dashboard        | Streamlit + Plotly                 |
| Testing          | pytest + responses (mock HTTP)     |

---

## Data Sources

| Source                       | Endpoint                                                             | Update cadence |
| ---------------------------- | -------------------------------------------------------------------- | -------------- |
| Caltrans CWWP2 chain control | `cwwp2.dot.ca.gov/data/d3/cc/ccStatusD03.json`                       | 1–5 min        |
| Caltrans CWWP2 RWIS weather  | `cwwp2.dot.ca.gov/data/d3/rwis/rwisStatusD03.json`                   | 5 min          |
| NWS hourly forecast          | `api.weather.gov/gridpoints/REV/{x},{y}/forecast/hourly`             | Hourly         |
| Open-Meteo current           | `api.open-meteo.com/v1/forecast`                                     | Hourly         |
| Open-Meteo archive           | `archive-api.open-meteo.com/v1/archive`                              | Historical     |
| USGS earthquakes (live)      | `earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` | 1 min          |
| USGS FDSN catalog (backfill) | `earthquake.usgs.gov/fdsnws/event/1/query`                           | Historical     |
| CCRS crash records           | `data.ca.gov/dataset/ccrs`                                           | Daily          |

---


## Streaming Pipeline

The streaming path uses a **Kafka producer → Spark Structured Streaming consumer** architecture.

### Producer (`ingestion/producer.py`)

- Runs on a 5-minute poll loop
- Calls all 6 data sources in parallel (Caltrans CWWP2, NWS, Open-Meteo, USGS, CCRS)
- Applies the safety score formula to produce a scored event per segment
- Publishes each event as an Avro-serialized message to the `sierra.road.events` Kafka topic
- Kafka broker + Zookeeper + Schema Registry run locally via Docker Compose

### Consumer (`streaming/spark_consumer.py`)

- Spark Structured Streaming job reads from the `sierra.road.events` Kafka topic
- Computes **15-minute tumbling window aggregates** (avg/min/max score, dominant band, event count) per segment using Spark's native windowing — available on the streaming DataFrame for in-stream monitoring and debug sinks
- Writes each micro-batch of raw events to GCS as Parquet (partitioned by date) using Spark's `foreachBatch`
- Loads those Parquet files from GCS into the `raw_road_events` BigQuery table via a BigQuery load job — chosen over a streaming-insert connector so we stay off the streaming-insert quota and keep precise control over the table's schema, DAY partitioning, and clustering
- dbt picks up `raw_road_events` on its next scheduled run and rebuilds the hourly window aggregates in `int_segment_windows`

In total, this includes a live Kafka broker, a Python producer publishing scored events, and Spark Structured Streaming consuming and aggregating them before writing to the data warehouse.

---

## Setup

See [SETUP.md](SETUP.md) for prerequisites, GCP project setup, step-by-step install instructions, backfill, Airflow orchestration, and validation.

---

## Corridor Segments

| Segment | Name             | Route  | Elevation |
| ------- | ---------------- | ------ | --------- |
| SEG_01  | Sacramento       | I-80   | 30 ft     |
| SEG_02  | Roseville        | I-80   | 164 ft    |
| SEG_03  | Auburn           | I-80   | 1,255 ft  |
| SEG_04  | Colfax           | I-80   | 2,421 ft  |
| SEG_05  | Emigrant_Gap     | I-80   | 5,224 ft  |
| SEG_06  | Donner_Summit    | I-80   | 7,227 ft  |
| SEG_07  | Truckee          | I-80   | 5,817 ft  |
| SEG_08  | Placerville      | US-50  | 1,867 ft  |
| SEG_09  | Echo_Summit      | US-50  | 7,382 ft  |
| SEG_10  | South_Lake_Tahoe | US-50  | 6,237 ft  |
| SEG_11  | Jackson          | HWY-88 | 1,200 ft  |
| SEG_12  | Pioneer          | HWY-88 | 3,300 ft  |
| SEG_13  | Carson_Spur      | HWY-88 | 7,990 ft  |
| SEG_14  | Kirkwood         | HWY-88 | 7,800 ft  |
| SEG_15  | Carson_Pass      | HWY-88 | 8,573 ft  |

---

## Dashboard Tiles

**Tile 1 — Current Conditions & Historical Crash Context** (`raw_road_events` + `mart_crash_conditions`)
The first thing you see: current sensor readings per segment (last 3 hours) — chain control status, snowfall rate, visibility, wind gusts, surface temperature, and seismic activity — alongside historical crash counts at each location from the CHP CCRS/SWITRS database (2024–2025). **Crashes (historical)** is the total reported collisions near each segment over the selected date range; it measures how crash-prone a location has been in the past, not a live incident count. Use it alongside the safety score to calibrate risk: a segment with many past crashes and a low score today warrants extra caution.

**Tile 2 — Safety Score Over Time** (`mart_safety_score_history`)
Line chart of avg score per segment over the selected date range. Answers "was Donner Summit worse on Tuesday or Wednesday?"

**Tile 3 — Crash Hotspot Map** (`mart_crash_conditions` / `crashes`)
Scatter map of CCRS crash locations colored by route. Overlays corridor waypoints.

### Screenshots

![Tile 1 — Current Conditions & Historical Crash Context](docs/images/Tile1.png)

![Tiles 2 & 3 — Safety Score Over Time and Crash Hotspot Map](docs/images/Tile2and3.png)

---

## Safety Score Formula

Scores run from 0 (road closed / extreme danger) to 100 (ideal conditions).

```
score = 100

# Chain control (Caltrans CWWP2)
if R3: score -= 60    # chains required all vehicles, road likely closing
if R2: score -= 30    # chains required all vehicles
if R1: score -= 15    # chains required except 4WD+snow tires
if closed: score = 0  # hard override

# Snowfall rate (Open-Meteo, inches/hr)
score -= min(40, snowfall_rate_in_hr * 20)

# Visibility (RWIS sensor or NWS, miles)
if visibility < 0.25: score -= 25
elif visibility < 0.5: score -= 15
elif visibility < 1.0: score -= 8

# Wind gusts (NWS or RWIS, mph)
if gusts > 60: score -= 20
elif gusts > 40: score -= 10
elif gusts > 30: score -= 5

# Road surface temp (RWIS, °C)
if surface_temp < -4: score -= 15   # black ice likely
elif surface_temp < 0: score -= 8   # freezing

# Seismic (USGS, within 80 km of segment)
if mag >= 4.0: score -= 15
elif mag >= 3.0: score -= 5

# Crashes (CCRS, within 16 km, last 24 hours)
if Fatal:        score -= 20
elif Severe:     score -= 12
elif Injury:     score -= 6
else PDO:        score -= min(3 * count, 9)
# max crash penalty capped at 25

score = max(0, min(100, score))
```

**Score bands:** 80–100 GREEN · 60–79 YELLOW · 40–59 ORANGE · 20–39 RED · 0–19 BLACK

---

## Known Limitations

- **Spark JVM**: The full `spark_consumer.py` requires Java 17+ with JAVA_HOME and HADOOP_HOME set (Windows: also requires winutils).
- **dbt**: Requires live BigQuery credentials. `dbt debug` verifies the connection before `dbt run`.
- **CCRS crash data**: Requires a manual download step (`python -m ingestion.sources.ccrs_client --years 2024 2025`) before the crash tiles appear. The dashboard operates without crash data but the Tile 1 historical crashes column will be empty.
- **Scores are approximate**: Only as accurate as the source data. RWIS sensors may be offline; NWS grid coverage at highest elevations is coarser than valley stations. Chain control inference for historical backfill is derived from snowfall rate.
- **Geographic coverage is limited to the Sacramento–Tahoe corridors** (I-80 to Truckee, US-50 to South Lake Tahoe, HWY-88 to Carson Pass). The Reno/Sun Valley side of the Sierra and the full Lake Tahoe basin are not yet covered; extending the waypoint set is the primary planned expansion.

## Future Plans

**Predictive Power**

- Surface NWS and Open-Meteo multi-day forecasts (next 24 h, 48 h, 72 h) as forward-looking safety score estimates alongside the current real-time score
- Build a forecast accuracy tile: for each past forecast window, compare the predicted score to the observed score once actuals arrived — surfacing mean absolute error per segment and per condition type (snowstorm, wind event, clear)
- Use the historical record to ask "Given today's NWS forecast, how dangerous have similar forecast days actually been?" — e.g., a 6-hour-ahead forecast of R2 chain control correlates with an observed average score of X ± Y
- Long-term goal: train a lightweight regression model on the historical time-series (weather forecast → observed score) to produce calibrated probability bands (e.g., "70% chance score drops below 40 at Donner Summit between 6–9 am")

**Coverage**

- Expand waypoints to additional areas along I-80, US-50, HWY-89, and HWY-88
- Expand beyond California to Nevada


## Acknowledgements

Thank you to [Alexey Grigorev](https://github.com/alexeygrigorev) and the entire [DataTalks.Club](https://datatalks.club) team for building and maintaining the Data Engineering Zoomcamp. This project was built as the capstone for the [DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) (January–April 2026 cohort). The course covered the full modern data engineering stack — cloud infrastructure, workflow orchestration, stream processing, data warehousing, and transformation pipelines — and the capstone integrates all of those components into a single project. The course provided a clear, hands-on path through every layer of the modern data stack, and the open, community-driven format made it genuinely accessible. This project would not exist without it.
