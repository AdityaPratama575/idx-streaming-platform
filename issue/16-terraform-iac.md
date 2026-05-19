# Issue 16: Infrastructure as Code — Terraform

## Tujuan
Mendefinisikan seluruh infrastruktur GCP menggunakan Terraform agar reproducible, version-controlled, dan bisa di-deploy ke environment manapun dengan satu command.

---

## Spesifikasi

### A. Resource yang Harus Di-manage Terraform

#### GCP Project & APIs

```hcl
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
```

#### BigQuery Dataset

```hcl
resource "google_bigquery_dataset" "idx_stock_data" {
  dataset_id  = "idx_stock_data"
  location    = "asia-southeast2"  # Jakarta region
  description = "IDX real-time stock data pipeline"
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
```

#### BigQuery Tables

| Table | Type | Partition | Cluster |
|---|---|---|---|
| `top_sector_ticks` | Raw (managed by Spark) | `timestamp` (DAY) | `ticker`, `sector` |
| `dq_check_results` | DQ results | `execution_ts` (DAY) | `check_name` |

#### GCS Buckets

```hcl
resource "google_storage_bucket" "temp_staging" {
  name     = "${var.project_id}-temp-staging"
  location = "asia-southeast2"
  
  lifecycle_rule {
    condition { age = 7 }
    action { type = "Delete" }
  }
}

resource "google_storage_bucket" "dbt_artifacts" {
  name     = "${var.project_id}-dbt-artifacts"
  location = "asia-southeast2"
}
```

#### Service Account & IAM

```hcl
resource "google_service_account" "idx_pipeline" {
  account_id   = "idx-pipeline-owner"
  display_name = "IDX Pipeline Service Account"
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
```

### B. Terraform Structure

```
terraform/
├── main.tf                    # Provider, APIs, project config
├── variables.tf               # All input variables
├── outputs.tf                 # Output values (SA email, bucket names)
├── terraform.tfvars.example   # Template variable values
├── bigquery.tf                # Dataset + tables definition
├── storage.tf                 # GCS buckets
├── iam.tf                     # Service accounts + permissions
└── versions.tf                # Terraform & provider version constraints
```

### C. Variables (`variables.tf`)

| Variable | Type | Default | Deskripsi |
|---|---|---|---|
| `project_id` | string | — | GCP Project ID |
| `region` | string | `asia-southeast2` | GCP region |
| `environment` | string | `dev` | dev/staging/prod |
| `bq_dataset_name` | string | `idx_stock_data` | BigQuery dataset ID |
| `data_retention_days` | number | `90` | Retention raw data (hari) |

### D. Remote State (optional for production)

```hcl
terraform {
  backend "gcs" {
    bucket = "idx-terraform-state"
    prefix = "terraform/state"
  }
}
```

### E. Usage

```bash
# Init
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars dengan GCP project ID
terraform init
terraform plan
terraform apply

# Output service account key
terraform output -raw service_account_key > ../gcp-service-account.json
```

---

## Acceptance Criteria

- [ ] `terraform init` + `terraform plan` berhasil tanpa error
- [ ] `terraform apply` membuat semua resource di GCP
- [ ] BigQuery dataset + tabel terbuat dengan schema yang benar
- [ ] GCS bucket terbuat dengan lifecycle rule 7 hari
- [ ] Service account dibuat dengan IAM role `bigquery.dataEditor` + `storage.objectAdmin`
- [ ] `terraform output` menghasilkan key JSON service account yang valid
- [ ] `.gitignore` menambahkan `terraform.tfvars`, `*.tfstate`, `*.tfstate.backup`
- [ ] Tidak ada hardcoded project ID di file `.tf` (semua dari variables)
