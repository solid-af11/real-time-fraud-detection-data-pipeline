"""
dags/fraud_model_retraining.py
Daily model retraining DAG — runs at 02:00 UTC.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud123@postgres/fraud_detection")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MIN_TRAINING_ROWS = int(os.getenv("MIN_TRAINING_ROWS", "10000"))
MODEL_NAME = "fraud-classifier"

default_args = {
    "owner": "fraud-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def data_quality_check(**context) -> bool:
    import psycopg2
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE timestamp >= NOW() - INTERVAL '30 days'
        """)
        row_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE timestamp >= NOW() - INTERVAL '30 days'
            AND is_fraud = TRUE
        """)
        fraud_count = cur.fetchone()[0]

        log.info(f"[quality] {row_count:,} rows, {fraud_count:,} fraud examples")
        context["ti"].xcom_push(key="row_count", value=row_count)
        context["ti"].xcom_push(key="fraud_count", value=fraud_count)

        if row_count < MIN_TRAINING_ROWS:
            log.warning(f"[quality] Only {row_count:,} rows (min: {MIN_TRAINING_ROWS:,}). Skipping.")
            return False
        if fraud_count < 100:
            log.warning(f"[quality] Only {fraud_count} fraud examples. Skipping.")
            return False
        return True
    finally:
        conn.close()


def log_retrain_start(**context):
    import psycopg2
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO model_training_log (run_date, dag_run_id, status)
            VALUES (NOW(), %s, 'started')
            RETURNING id
        """, (context["run_id"],))
        log_id = cur.fetchone()[0]
        context["ti"].xcom_push(key="log_id", value=log_id)
        log.info(f"[retrain] Audit log created: id={log_id}")
    finally:
        conn.close()


def run_spark_training(**context):
    import docker
    client = docker.from_env()
    try:
        log.info("[retrain] Starting training job...")
        container = client.containers.get("feature-engineering")
        exit_code, output = container.exec_run(
            cmd=["python", "-u", "/opt/airflow/spark_jobs/model_training.py"],
            environment={
                "POSTGRES_DSN": POSTGRES_DSN,
                "MLFLOW_TRACKING_URI": MLFLOW_URI,
                "TRAINING_DAYS": "30",
            },
            stream=False,
        )
        output_str = output.decode("utf-8") if output else ""
        log.info(f"[retrain] Output:\n{output_str}")

        if exit_code != 0:
            raise RuntimeError(f"Training failed (exit {exit_code}):\n{output_str}")

        metrics = {}
        for line in output_str.split("\n"):
            for metric in ["roc_auc", "f1", "precision", "recall"]:
                if f"{metric}:" in line:
                    try:
                        val = float(line.split(f"{metric}:")[-1].strip())
                        metrics[metric] = val
                    except ValueError:
                        pass

        context["ti"].xcom_push(key="training_metrics", value=metrics)
        log.info(f"[retrain] Metrics: {metrics}")
    finally:
        client.close()


def verify_model_registered(**context):
    import urllib.request
    import urllib.error
    url = f"{MLFLOW_URI}/api/2.0/mlflow/registered-models/get?name={MODEL_NAME}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            latest = data.get("registered_model", {}).get("latest_versions", [])
            if not latest:
                raise RuntimeError(f"No versions found for model '{MODEL_NAME}'.")
            version = latest[0]["version"]
            stage = latest[0]["current_stage"]
            log.info(f"[retrain] Model verified: v{version}, stage={stage}")
            context["ti"].xcom_push(key="model_version", value=version)
    except urllib.error.URLError as exc:
        log.warning(f"[retrain] MLflow not reachable: {exc}. Skipping verification.")


def reload_inference_model(**context):
    import redis as redis_lib
    REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
    r = redis_lib.from_url(REDIS_URL, decode_responses=True)
    model_version = context["ti"].xcom_pull(key="model_version", task_ids="verify_model_registered")
    metrics = context["ti"].xcom_pull(key="training_metrics", task_ids="run_spark_training") or {}
    signal = {
        "reload_at": datetime.now(timezone.utc).isoformat(),
        "model_version": model_version,
        "metrics": metrics,
    }
    r.set("model_reload_signal", json.dumps(signal), ex=3600)
    log.info(f"[retrain] Reload signal written to Redis: {signal}")


def log_retrain_complete(**context):
    import psycopg2
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        log_id = context["ti"].xcom_pull(key="log_id", task_ids="log_retrain_start")
        metrics = context["ti"].xcom_pull(key="training_metrics", task_ids="run_spark_training") or {}
        cur.execute("""
            UPDATE model_training_log
            SET status = 'completed', roc_auc = %s, f1 = %s, notes = %s
            WHERE id = %s
        """, (metrics.get("roc_auc"), metrics.get("f1"), json.dumps(metrics), log_id))
        log.info(f"[retrain] Audit log updated: id={log_id}")
    finally:
        conn.close()


with DAG(
    dag_id="fraud_model_retraining",
    description="Daily fraud model retraining and MLflow registration",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "ml", "training"],
) as dag:

    quality_check = ShortCircuitOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    log_start = PythonOperator(
        task_id="log_retrain_start",
        python_callable=log_retrain_start,
    )

    spark_training = PythonOperator(
        task_id="run_spark_training",
        python_callable=run_spark_training,
        execution_timeout=timedelta(hours=2),
    )

    verify_model = PythonOperator(
        task_id="verify_model_registered",
        python_callable=verify_model_registered,
    )

    reload_model = PythonOperator(
        task_id="reload_inference_model",
        python_callable=reload_inference_model,
    )

    log_complete = PythonOperator(
        task_id="log_retrain_complete",
        python_callable=log_retrain_complete,
        trigger_rule="all_done",
    )

    quality_check >> log_start >> spark_training >> verify_model >> reload_model >> log_complete
