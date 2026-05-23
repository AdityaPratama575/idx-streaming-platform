variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-southeast2"
}

variable "environment" {
  description = "Environment name (dev/staging/prod)"
  type        = string
  default     = "dev"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "idx_stock_data"
}

variable "data_retention_days" {
  description = "Data retention in days for raw table"
  type        = number
  default     = 90
}
