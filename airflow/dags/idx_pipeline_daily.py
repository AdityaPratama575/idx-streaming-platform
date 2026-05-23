from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="idx_pipeline_daily",
    default_args=default_args,
    description="IDX Pipeline: fetch → process → transform → report",
    schedule="0 1 * * 1-5",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["idx", "pipeline"],
)

def fetch_stock_data(**context):
    import subprocess
    result = subprocess.run(
        ["python", "producer.py"],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Producer failed: {result.stderr}")
    return result.stdout

fetch_task = PythonOperator(
    task_id="fetch_stock_data",
    python_callable=fetch_stock_data,
    dag=dag,
)

class BigQueryFreshnessSensor(BaseSensorOperator):
    def __init__(self, project_id, dataset, table, **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataset = dataset
        self.table = table

    def poke(self, context):
        from google.cloud import bigquery
        client = bigquery.Client(project=self.project_id)
        query = f"""
            SELECT MAX(timestamp) as max_ts
            FROM `{self.project_id}.{self.dataset}.{self.table}`
            WHERE DATE(timestamp) = CURRENT_DATE()
        """
        result = client.query(query).result()
        for row in result:
            if row.max_ts and (datetime.utcnow() - row.max_ts).total_seconds() < 600:
                return True
        return False

wait_sensor = BigQueryFreshnessSensor(
    task_id="wait_for_spark",
    project_id="{{ var.value.gcp_project_id }}",
    dataset="idx_stock_data",
    table="top_sector_ticks",
    timeout=1800,
    poke_interval=120,
    mode="poke",
    dag=dag,
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
    dag=dag,
)

def data_quality_report(**context):
    from google.cloud import bigquery
    client = bigquery.Client()
    query = """
        SELECT check_name, failed_rows, failure_rate
        FROM dq_check_results
        WHERE DATE(execution_ts) = CURRENT_DATE()
        ORDER BY failure_rate DESC
        LIMIT 10
    """
    result = client.query(query).result()
    for row in result:
        print(f"DQ: {row.check_name} — {row.failed_rows} failures ({row.failure_rate:.2%})")

dq_report = PythonOperator(
    task_id="data_quality_report",
    python_callable=data_quality_report,
    dag=dag,
)

fetch_task >> wait_sensor >> dbt_run >> dbt_test >> dq_report
