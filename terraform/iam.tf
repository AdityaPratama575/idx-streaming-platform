resource "google_service_account" "idx_pipeline" {
  account_id   = "idx-pipeline-owner"
  display_name = "IDX Pipeline Service Account"
  description  = "Service account for IDX real-time pipeline"
}

resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.idx_pipeline.email}"
}

resource "google_project_iam_member" "gcs_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.idx_pipeline.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.idx_pipeline.email}"
}
