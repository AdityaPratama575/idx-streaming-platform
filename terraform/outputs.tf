output "project_id" {
  value = var.project_id
}

output "service_account_email" {
  value = google_service_account.idx_pipeline.email
}

output "service_account_key" {
  value     = base64decode(google_service_account.idx_pipeline.key.private_key)
  sensitive = true
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.idx_stock_data.dataset_id
}

output "temp_staging_bucket" {
  value = google_storage_bucket.temp_staging.name
}
