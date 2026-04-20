output "raw_bucket_name" {
  description = "Name of the raw GCS bucket"
  value       = google_storage_bucket.raw.name
}

output "processed_bucket_name" {
  description = "Name of the processed GCS bucket"
  value       = google_storage_bucket.processed.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.sierra_safety.dataset_id
}

output "raw_events_table_id" {
  description = "BigQuery raw_road_events table ID"
  value       = google_bigquery_table.raw_road_events.table_id
}

output "safety_scores_table_id" {
  description = "BigQuery safety_scores table ID"
  value       = google_bigquery_table.safety_scores.table_id
}
