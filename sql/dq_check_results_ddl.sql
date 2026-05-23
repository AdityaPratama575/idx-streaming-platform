CREATE TABLE IF NOT EXISTS dq_check_results (
    check_id        STRING,
    check_name      STRING,
    stage           STRING,
    batch_id        STRING,
    execution_ts    TIMESTAMP,
    total_rows      INT64,
    failed_rows     INT64,
    failure_rate    FLOAT64,
    details         STRING,
    severity        STRING
)
PARTITION BY DATE(execution_ts)
CLUSTER BY check_name;
