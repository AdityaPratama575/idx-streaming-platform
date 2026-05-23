from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="idx_streaming_monitor",
    default_args=default_args,
    description="Monitor pipeline health every 10 minutes",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["idx", "monitoring"],
)

def check_kafka_lag(**context):
    import json, subprocess
    result = subprocess.run(
        ["docker", "exec", "idx-kafka",
         "kafka-run-class", "kafka.tools.GetOffsetShell",
         "--bootstrap-server", "kafka:29092",
         "--topic", "idx_sector_ticks", "--time", "-1"],
        capture_output=True, text=True, timeout=30,
    )
    print(f"Kafka offsets: {result.stdout.strip()}")

def check_bigquery_freshness(**context):
    from google.cloud import bigquery
    client = bigquery.Client()
    query = "SELECT MAX(timestamp) as max_ts FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`"
    result = client.query(query).result()
    for row in result:
        if row.max_ts:
            age = (datetime.utcnow() - row.max_ts).total_seconds()
            print(f"Last data: {row.max_ts} ({age/60:.1f} min ago)")
            if age > 900:
                raise ValueError(f"Data too old: {age/60:.1f} min")

def check_container_health(**context):
    import subprocess
    result = subprocess.run(
        ["docker", "ps", "--filter", "status=exited", "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=30,
    )
    if result.stdout.strip():
        raise RuntimeError(f"Containers down: {result.stdout.strip()}")
    print("All containers healthy")

kafka_check = PythonOperator(task_id="check_kafka_lag", python_callable=check_kafka_lag, dag=dag)
bq_check = PythonOperator(task_id="check_bigquery_freshness", python_callable=check_bigquery_freshness, dag=dag)
health_check = PythonOperator(task_id="check_container_health", python_callable=check_container_health, dag=dag)

[kafka_check, bq_check, health_check]
