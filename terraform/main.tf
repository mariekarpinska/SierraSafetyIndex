terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# GCS Buckets
# ---------------------------------------------------------------------------

resource "google_storage_bucket" "raw" {
  name          = "sierra-safety-raw-${var.project_id}"
  location      = var.region
  force_destroy = false

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 90 }
  }

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "processed" {
  name          = "sierra-safety-processed-${var.project_id}"
  location      = var.region
  force_destroy = false

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 365 }
  }

  uniform_bucket_level_access = true
}

# ---------------------------------------------------------------------------
# BigQuery dataset
# ---------------------------------------------------------------------------

resource "google_bigquery_dataset" "sierra_safety" {
  dataset_id                 = var.bigquery_dataset
  location                   = var.region
  description                = "Sierra Nevada I-80 Drive Safety Index dataset"
  delete_contents_on_destroy = false
}

# ---------------------------------------------------------------------------
# IAM: BigQuery service account can read GCS processed bucket
# (needed for load_table_from_uri)
# ---------------------------------------------------------------------------

data "google_project" "project" {
  project_id = var.project_id
}

resource "google_storage_bucket_iam_member" "bq_reads_processed" {
  bucket = google_storage_bucket.processed.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:bq-${data.google_project.project.number}@bigquery-encryption.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "bq_reads_raw" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:bq-${data.google_project.project.number}@bigquery-encryption.iam.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# BigQuery tables
# ---------------------------------------------------------------------------

resource "google_bigquery_table" "raw_road_events" {
  dataset_id          = google_bigquery_dataset.sierra_safety.dataset_id
  table_id            = "raw_road_events"
  deletion_protection = false

  # Partitioned by event date for efficient time-range queries
  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  # Clustered by segment for per-segment queries (dashboard + dbt)
  clustering = ["segment_id"]

  schema = jsonencode([
    { name = "segment_id",          type = "STRING",    mode = "REQUIRED" },
    { name = "segment_name",        type = "STRING",    mode = "NULLABLE" },
    { name = "route",               type = "STRING",    mode = "NULLABLE" },
    { name = "lat",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "lon",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "elevation_ft",        type = "INT64",     mode = "NULLABLE" },
    { name = "event_timestamp",     type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "score",               type = "INT64",     mode = "NULLABLE" },
    { name = "band",                type = "STRING",    mode = "NULLABLE" },
    { name = "active_penalties",    type = "STRING",    mode = "REPEATED" },
    { name = "chain_control",       type = "STRING",    mode = "NULLABLE" },
    { name = "road_closed",         type = "BOOL",      mode = "NULLABLE" },
    { name = "snowfall_rate_in_hr", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "visibility_miles",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "wind_gust_mph",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "surface_temp_c",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "seismic_mag",         type = "FLOAT64",   mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "safety_scores" {
  dataset_id          = google_bigquery_dataset.sierra_safety.dataset_id
  table_id            = "safety_scores"
  deletion_protection = false

  # Partitioned by window date; clustered by segment + band for efficient dashboard queries
  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }

  clustering = ["segment_id", "dominant_band"]

  schema = jsonencode([
    { name = "window_start",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end",     type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "segment_id",     type = "STRING",    mode = "REQUIRED" },
    { name = "avg_score",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "min_score",      type = "INT64",     mode = "NULLABLE" },
    { name = "max_score",      type = "INT64",     mode = "NULLABLE" },
    { name = "event_count",    type = "INT64",     mode = "NULLABLE" },
    { name = "dominant_band",  type = "STRING",    mode = "NULLABLE" }
  ])
}

# Crash records from CCRS/SWITRS
# Partitioned by collision date; clustered by segment proximity (segment_id)
# and severity for crash-condition matching queries
resource "google_bigquery_table" "crashes" {
  dataset_id          = google_bigquery_dataset.sierra_safety.dataset_id
  table_id            = "crashes"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "collision_datetime"
  }

  clustering = ["severity"]

  schema = jsonencode([
    { name = "case_id",             type = "STRING",    mode = "NULLABLE" },
    { name = "collision_datetime",  type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "lat",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "lon",                 type = "FLOAT64",   mode = "NULLABLE" },
    { name = "severity",            type = "STRING",    mode = "NULLABLE" },
    { name = "collision_type",      type = "STRING",    mode = "NULLABLE" },
    { name = "primary_factor",      type = "STRING",    mode = "NULLABLE" },
    { name = "vehicle_type",        type = "STRING",    mode = "NULLABLE" },
    { name = "route",               type = "STRING",    mode = "NULLABLE" }
  ])
}
