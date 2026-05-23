resource "google_storage_bucket" "temp_staging" {
  name     = "${var.project_id}-temp-staging"
  location = var.region
  force_destroy = false
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "dbt_artifacts" {
  name     = "${var.project_id}-dbt-artifacts"
  location = var.region
  force_destroy = false
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
