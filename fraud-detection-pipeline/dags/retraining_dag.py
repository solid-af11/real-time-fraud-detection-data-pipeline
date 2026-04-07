"""
retraining_dag.py
Airflow DAG — triggers the Spark model retraining job daily at 02:00 UTC.

Pipeline:
  1. data_quality_check  — verify minimum row count in transactions table
  2. run_spark_training  — submit spark_jobs/model_training.py
  3. notify_slack        — post summary to Slack (optional)
  4. update_dashboard    — refresh Grafana annotation
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
import psycopg2

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://fraud:fraud123@postgres/fraud_detection"
)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MIN_TRAINING_ROWS = int(os.getenv("MIN_TRAINING_ROWS", "10000"))

default_args = {
    "owner": "fraud-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ──────────────────────────────────────────────────────────────────────────────
# Task functions
# ──────────────────────────────────────────────────────────────────────────────

def check_data_quality(**context) -> bool:
    """Return True only if we have enough rows for a meaningful retrain."""
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) FROM transactions
        WHERE timestamp >= NOW() - INTERVAL '30 days'
        """
    )
    row_count = cur.fetchone()[0]
    conn.close()

    print(f"[quality-check] Found {row_count:,} training rows (min: {MIN_TRAINING_ROWS:,})")
    if row_count < MIN_TRAINING_ROWS:
        print("[quality-check] Insufficient data — skipping retrain.")
        return False
    return True


def log_retrain_event(**context):
    """Record retraining event metadata to the audit table in Postgres."""
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO model_training_log (run_date, dag_run_id, status)
        VALUES (NOW(), %s, 'triggered')
        """,
        (context["run_id"],),
    )
    conn.commit()
    conn.close()
    print(f"[log] Retraining event recorded for run {context['run_id']}")


# ──────────────────────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="fraud_model_retraining",
    description="Daily fraud model retrain + MLflow registration",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "ml", "training"],
) as dag:

    data_quality_check = ShortCircuitOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
    )

    log_event = PythonOperator(
        task_id="log_retrain_event",
        python_callable=log_retrain_event,
    )

    run_spark_training = BashOperator(
        task_id="run_spark_training",
        bash_command="""
            spark-submit \
              --master local[4] \
              --packages org.postgresql:postgresql:42.7.1 \
              --conf spark.mlflow.trackingUri={{ var.value.get('mlflow_uri', 'http://mlflow:5000') }} \
              /opt/airflow/spark_jobs/model_training.py
        """,
        env={
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "POSTGRES_DSN": POSTGRES_DSN,
        },
    )

    verify_model_registered = BashOperator(
        task_id="verify_model_registered",
        bash_command="""
            python3 -c "
import mlflow
mlflow.set_tracking_uri('{{ var.value.get(\"mlflow_uri\", \"http://mlflow:5000\") }}')
client = mlflow.tracking.MlflowClient()
versions = client.get_latest_versions('fraud-classifier')
if not versions:
    raise ValueError('No model versions found after training!')
print(f'Latest model version: {versions[0].version}')
"
        """,
    )

    update_training_log_success = PythonOperator(
        task_id="update_training_log_success",
        python_callable=lambda **ctx: (
            psycopg2.connect(POSTGRES_DSN)
        ),
        # In a real pipeline you'd update the log record to 'success'
    )

    # DAG dependency chain
    data_quality_check >> log_event >> run_spark_training >> verify_model_registered
