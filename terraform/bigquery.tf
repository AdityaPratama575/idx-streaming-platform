resource "google_project_service" "services" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "storage.googleapis.com",
    "iamcredentials.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  service = each.key
}

resource "google_bigquery_dataset" "idx_stock_data" {
  dataset_id  = var.bq_dataset_name
  location    = var.region
  description = "IDX real-time stock data pipeline"
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
  delete_contents_on_destroy = false
}

resource "google_bigquery_table" "top_sector_ticks" {
  dataset_id = google_bigquery_dataset.idx_stock_data.dataset_id
  table_id   = "top_sector_ticks"
  schema     = file("${path.module}/schemas/top_sector_ticks_schema.json")
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["ticker", "sector"]
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
