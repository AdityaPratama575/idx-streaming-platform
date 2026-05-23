import os
import json
import tempfile
from google.cloud import secretmanager


def get_secret(secret_id: str, version: str = "latest") -> str:
    """Fetch secret from GCP Secret Manager with .env fallback."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.getenv("GCP_PROJECT_ID")
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception:
        pass
    value = os.getenv(secret_id.upper())
    if value:
        return value
    raise RuntimeError(f"Secret '{secret_id}' not found in Secret Manager or .env")


def write_service_account_to_temp():
    """Write SA JSON to temp file and return path."""
    sa_json = get_secret("idx-pipeline-service-account")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(sa_json)
        return f.name
