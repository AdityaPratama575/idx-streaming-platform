#!/usr/bin/env python3
"""Setup BigQuery dataset and tables for IDX-Stream pipeline."""
import os, json
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-service-account.json"
client = bigquery.Client()

dataset_id = "idx_stock_data"
table_id = f"{client.project}.{dataset_id}.top_sector_ticks"
bucket_name = f"{client.project}-temp-staging"

# Create dataset
dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
dataset.location = "asia-southeast2"
client.create_dataset(dataset, exists_ok=True)
print(f"Dataset {dataset_id} ready")

# Create table
schema = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("sector", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("fetch_ts", "TIMESTAMP"),
    bigquery.SchemaField("open", "FLOAT"),
    bigquery.SchemaField("high", "FLOAT"),
    bigquery.SchemaField("low", "FLOAT"),
    bigquery.SchemaField("close", "FLOAT"),
    bigquery.SchemaField("volume", "INTEGER"),
]
table = bigquery.Table(table_id, schema=schema)
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="timestamp",
)
table.clustering_fields = ["ticker", "sector"]
client.create_table(table, exists_ok=True)
print(f"Table {table_id} ready (partitioned + clustered)")
print(f"\nGCS bucket needed: {bucket_name}")
print("Create it at: https://console.cloud.google.com/storage")
