# Sierra Safety Index — Data Model

![ERD Diagram](images/ERD.PNG)

The pipeline follows a **medallion architecture** (Bronze → Silver → Gold), with each layer progressively refining raw data into dashboard-ready metrics.

| Layer      | Contents                                                                                                                                                                                                                     | Materialisation                           |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| **Bronze** | `raw_road_events`, `safety_scores`, `crashes` — raw BigQuery tables written directly by Spark and the backfill/CCRS scripts. No transformations; append-only.                                                                | BigQuery tables (partitioned + clustered) |
| **Silver** | `stg_road_events`, `stg_safety_scores` — dbt staging views that enforce correct types and a stable schema contract. `int_segment_windows` joins and hourly-aggregates both staging sources into a single clean intermediate. | dbt views / intermediate                  |
| **Gold**   | `mart_safety_score_history`, `mart_segment_risk_profile`, `mart_crash_conditions` — dbt mart tables consumed by the Streamlit dashboard.                                                                                     | dbt incremental / full-refresh tables     |

The pipeline writes to three raw BigQuery tables and transforms them through four dbt layers before the dashboard reads anything. Dashboard queries go through dbt mart models.

---

## BigQuery Source Tables

- **`raw_road_events`** — one row per segment per 5-minute poll cycle. The primary fact table: stores every sensor reading (snowfall rate, visibility, wind gusts, surface temp, seismic magnitude, chain control status) alongside the computed safety score for that moment. Written by Spark (live streaming) and `ingestion/backfill.py` (historical seed).
- **`safety_scores`** — optional 15-minute tumbling window aggregates (avg/min/max score, dominant band, event count per segment). When present, dbt reads it directly; when absent, `int_segment_windows` recomputes the equivalent aggregates from `raw_road_events` via a LEFT JOIN fallback, so the pipeline works either way.
- **`crashes`** — CCRS/SWITRS crash records filtered to the corridor (I-80, US-50, HWY-88), within 8 km of a waypoint. Written by a separate daily batch process. Contains crash location, severity, collision type, and primary factor.

---

## dbt Staging Views

- **`stg_road_events`** — casts every column in `raw_road_events` to its correct type (score → INT64, lat/lon → FLOAT64, event_timestamp → TIMESTAMP, road_closed → BOOL).
- **`stg_safety_scores`** — same pattern for `safety_scores`: casts window_start/window_end to TIMESTAMP, numeric fields to FLOAT64/INT64.

---

## dbt Intermediate View

- **`int_segment_windows`** — joins `stg_road_events` and `stg_safety_scores`, truncates events to hourly buckets, and aggregates ~12 readings/hour into avg/min/max score, snowfall event counts, and chain control activation counts per segment. Derives `dominant_band` from the Spark-computed value where available, falling back to recalculating from avg_score for backfill periods where `safety_scores` has no matching row. Segment metadata (name, lat, lon, elevation) is pulled from the events table rather than a static seed file. This is the single aggregation layer — both marts read from it, keeping aggregation logic in one place.

---

## dbt Mart Tables (dashboard-facing)

- **`mart_safety_score_history`** — one row per segment per hour, materialized as an **incremental** table (unique key: `segment_id` + `window_hour`). Each dbt run appends only hours not yet seen, making it efficient on a growing time-series. Feeds the time-series line chart (Tile 2) and band distribution chart (Tile 1).
- **`mart_segment_risk_profile`** — one row per segment across all time, full table rebuild on each dbt run. Computes percentage of hours spent in each safety band (GREEN/YELLOW/ORANGE/RED/BLACK), all-time average score, and the single worst recorded event (score + timestamp). Ordered by elevation descending so highest/most dangerous segments appear first.
- **`mart_crash_conditions`** — one row per crash record, enriched with the nearest segment and the road conditions (from `stg_road_events`) in the two hours before the crash. **Partitioned by DAY on `collision_datetime`**, clustered by `severity` and `segment_id`. Answers "what were the sensor readings when this crash happened?" Feeds the crash map (Tile 3) and the historical crash context in Tile 1. Materializes empty if `crashes` has no data.

---

## Partitioning and Clustering Strategy

BigQuery tables are partitioned and clustered to match the exact filter patterns used by dbt and the dashboard:

| Table                   | Partition                   | Cluster                  | Why                                                                                                                                                                                                                                                                                                                                                                                              |
| ----------------------- | --------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `raw_road_events`       | DAY on `event_timestamp`    | `segment_id`             | dbt incremental runs filter `WHERE window_hour > max(window_hour)` — DAY partitioning means only the most recent partition is scanned. Clustering by `segment_id` means per-segment queries (e.g. Donner Summit only) skip irrelevant row groups within that partition. Without this, every dbt run would full-scan the entire growing table.                                                    |
| `mart_crash_conditions` | DAY on `collision_datetime` | `severity`, `segment_id` | The dashboard always filters by date range (`WHERE collision_datetime BETWEEN ...`). DAY partitioning eliminates full table scans for date-bounded queries. Clustering by severity lets the dashboard filter to Fatal/Severe crashes first (the most operationally relevant) without scanning all crash records. Secondary cluster on `segment_id` supports per-segment crash lookups in Tile 1. |

`mart_safety_score_history` and `mart_segment_risk_profile` are not partitioned because they are already pre-aggregated to at most ~15 segments × hours/days — small enough that full scans are cheap and partitioning adds no benefit.
