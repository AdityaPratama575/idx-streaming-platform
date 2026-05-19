# Issue 20: Secrets Management

## Tujuan
Mengganti `.env` file approach dengan Google Cloud Secret Manager untuk production, dan menyediakan mekanisme secrets yang aman untuk semua environment (local dev, CI/CD, GKE).

---

## Spesifikasi

### A. Identifikasi Secrets

| Secret | Current Storage | Target Storage | Needs Rotation |
|---|---|---|---|
| `GCP_SERVICE_ACCOUNT_JSON` | File `.json` (gitignored) | GCP Secret Manager | Ya (setiap 90 hari) |
| `GCP_PROJECT_ID` | `.env` (gitignored) | GCP Secret Manager | Tidak (static) |
| `KAFKA_BOOTSTRAP_SERVERS` | `.env` | ConfigMap (not secret) | Tidak |
| `GRAFANA_PASSWORD` | `.env` | GCP Secret Manager | Ya (setiap 90 hari) |
| `AIRFLOW_PASSWORD` | `.env` | GCP Secret Manager | Ya (setiap 90 hari) |
| `GITHUB_TOKEN` (CI/CD) | `.env` | GitHub Secrets | Ya |

### B. Terraform untuk Secret Manager (tambahan Issue 16)

```hcl
# terraform/secrets.tf

resource "google_secret_manager_secret" "service_account" {
  secret_id = "idx-pipeline-service-account"
  replication {
    automatic = true
  }
  labels = { environment = var.environment }
}

resource "google_secret_manager_secret_version" "service_account" {
  secret      = google_secret_manager_secret.service_account.id
  secret_data = file("../gcp-service-account.json")
}

resource "google_secret_manager_secret" "grafana_password" {
  secret_id = "idx-grafana-password"
  replication { automatic = true }
}

resource "google_secret_manager_secret" "airflow_password" {
  secret_id = "idx-airflow-password"
  replication { automatic = true }
}

# IAM: service account bisa baca secret
resource "google_secret_manager_secret_iam_member" "pipeline_access" {
  for_each = toset([
    google_secret_manager_secret.service_account.secret_id,
    google_secret_manager_secret.grafana_password.secret_id,
    google_secret_manager_secret.airflow_password.secret_id,
  ])
  secret_id = each.key
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.idx_pipeline.email}"
}
```

### C. Runtime Secret Loading (Python)

Buat module `utils/secrets.py`:

```python
import os
from google.cloud import secretmanager

def get_secret(secret_id: str, version: str = "latest") -> str:
    """Fetch secret from GCP Secret Manager with .env fallback."""
    # Try Secret Manager first (production)
    try:
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.getenv("GCP_PROJECT_ID")
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception:
        pass
    
    # Fallback to .env (local development)
    value = os.getenv(secret_id.upper())
    if value:
        return value
    
    raise RuntimeError(f"Secret '{secret_id}' not found in Secret Manager or .env")
```

### D. Update `producer.py` dan `spark_processor.py`

```python
# Before (hardcode + .env)
CREDENTIAL_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/gcp-service-account.json")

# After (Secret Manager with fallback)
from utils.secrets import get_secret
import json, tempfile

sa_json = get_secret("idx-pipeline-service-account")
# Write to temp file for spark.hadoop.fs.gs.auth.service.account.keyfile
with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
    f.write(sa_json)
    cred_path = f.name
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
```

### E. Kubernetes Integration (Sealed Secrets / External Secrets)

Untuk GKE deployment (Issue 19), gunakan **External Secrets Operator**:

```yaml
# k8s/templates/external-secret.yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: idx-gcp-secrets
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: idx-secrets
  data:
  - secretKey: GCP_SERVICE_ACCOUNT_JSON
    remoteRef:
      key: idx-pipeline-service-account
```

### F. Local Development Secret Workflow

```bash
# Developer setup: copy .env.example → .env, isi manual
cp .env.example .env
# Edit .env dengan nilai lokal

# Script pre-commit hook: cek apakah .env tertarik ke staging
# .git/hooks/pre-commit
#!/bin/sh
if git diff --cached --name-only | grep -q "^.env$"; then
  echo "ERROR: .env file staged for commit! Run 'git reset HEAD .env'"
  exit 1
fi
```

### G. GitHub Secrets untuk CI/CD

Konfigurasi via GitHub Settings → Secrets and Variables → Actions:

| Secret Name | Deskripsi |
|---|---|
| `GCP_SERVICE_ACCOUNT_JSON` | Service account untuk CI/CD test + dbt run |
| `GHCR_TOKEN` | Token untuk push Docker image |
| `SLACK_WEBHOOK_URL` | Webhook untuk alert CI/CD failure |

### H. `.gitignore` Update

```
# Secrets Management
.env
.env.*
!.env.example
gcp-service-account.json
*-service-account.json
*.credentials.json
secrets/
```

---

## Acceptance Criteria

- [ ] Semua secret di-production di-fetch dari GCP Secret Manager (bukan .env)
- [ ] Terraform mendefinisikan semua Secret Manager resources
- [ ] `utils/secrets.py` menyediakan unified interface: Secret Manager → .env fallback
- [ ] Local development tetap bisa pakai `.env` tanpa Secret Manager
- [ ] Pre-commit hook mencegah `.env` ter-commit (jika belum di .gitignore)
- [ ] External Secrets Operator di GKE auto-sync secret dari GCP
- [ ] CI/CD pipeline menggunakan GitHub Secrets (tidak hardcode)
