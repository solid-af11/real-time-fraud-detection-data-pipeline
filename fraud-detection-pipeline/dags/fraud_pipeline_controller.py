"""
dags/fraud_pipeline_controller.py
Master orchestration DAG — runs every 5 minutes.
Checks all pipeline containers are running, verifies throughput,
and alerts if fraud rate exceeds 5%.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

PIPELINE_CONTAINERS = {
    "transaction-generator": {"max_restarts": 3, "critical": True},
    "feature-engineering":   {"max_restarts": 3, "critical": True},
    "fraud-inference":       {"max_restarts": 3, "critical": True},
    "alert-service":         {"max_restarts": 3, "critical": False},
}

INFRA_CONTAINERS = ["kafka", "redis", "postgres", "zookeeper"]

default_args = {
    "owner": "fraud-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_infrastructure(**context):
    import docker
    client = docker.from_env()
    failures = []
    for name in INFRA_CONTAINERS:
        try:
            container = client.containers.get(name)
            if not container.attrs["State"]["Running"]:
                failures.append(f"{name} is not running")
            else:
                log.info(f"[infra] {name}: OK")
        except docker.errors.NotFound:
            failures.append(f"{name}: not found")
    client.close()
    if failures:
        raise RuntimeError("Infrastructure check failed:\n" + "\n".join(f"  - {f}" for f in failures))
    log.info("[infra] All infrastructure containers healthy.")


def ensure_container_running(container_name: str, max_restarts: int = 3, **context):
    import docker
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        state = container.attrs["State"]
        restart_count = container.attrs.get("RestartCount", 0)
        is_running = state["Running"]

        log.info(f"[pipeline] {container_name}: running={is_running}, restarts={restart_count}")

        if is_running:
            context["ti"].xcom_push(
                key=f"{container_name}_status",
                value={"running": True, "restart_count": restart_count}
            )
            return

        if restart_count >= max_restarts:
            raise RuntimeError(
                f"Container '{container_name}' has failed {restart_count} times. "
                f"Manual intervention required."
            )

        log.warning(f"[pipeline] {container_name} is down. Restarting...")
        container.start()
        log.info(f"[pipeline] {container_name} restarted successfully.")
        context["ti"].xcom_push(
            key=f"{container_name}_status",
            value={"running": True, "restarted": True, "restart_count": restart_count + 1}
        )

    except docker.errors.NotFound:
        raise RuntimeError(f"Container '{container_name}' not found.")
    finally:
        client.close()


def check_pipeline_flow(**context):
    import psycopg2
    POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud123@postgres/fraud_detection")
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM transactions WHERE timestamp >= NOW() - INTERVAL '5 minutes'")
        tx_5min = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fraud_alerts WHERE alert_timestamp >= NOW() - INTERVAL '5 minutes'")
        alerts_5min = cur.fetchone()[0]

        log.info(f"[flow] Last 5 min: {tx_5min} transactions, {alerts_5min} fraud alerts")
        context["ti"].xcom_push(key="tx_5min", value=tx_5min)
        context["ti"].xcom_push(key="alerts_5min", value=alerts_5min)

        if tx_5min < 10:
            raise RuntimeError(
                f"Pipeline throughput too low: only {tx_5min} transactions in last 5 min. "
                f"Expected >10."
            )
        log.info(f"[flow] Pipeline throughput OK.")
    finally:
        conn.close()


def check_fraud_rate(**context):
    import psycopg2
    POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud123@postgres/fraud_detection")
    THRESHOLD = float(os.getenv("FRAUD_RATE_ALERT_THRESHOLD", "0.05"))
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM transactions WHERE timestamp >= NOW() - INTERVAL '1 hour'")
        total_tx = cur.fetchone()[0] or 0
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM fraud_alerts
            WHERE alert_timestamp >= NOW() - INTERVAL '1 hour'
        """)
        row = cur.fetchone()
        total_alerts = row[0] or 0
        total_amount = float(row[1] or 0)
        fraud_rate = total_alerts / total_tx if total_tx > 0 else 0.0

        log.info(f"[fraud-rate] {fraud_rate*100:.2f}% ({total_alerts}/{total_tx} tx), ${total_amount:,.2f}")
        context["ti"].xcom_push(key="fraud_rate", value=fraud_rate)
        context["ti"].xcom_push(key="total_alerts_1hr", value=total_alerts)
        context["ti"].xcom_push(key="total_amount_1hr", value=total_amount)

        if fraud_rate > THRESHOLD:
            raise ValueError(
                f"FRAUD RATE SPIKE: {fraud_rate*100:.2f}% exceeds "
                f"threshold {THRESHOLD*100:.1f}% — "
                f"{total_alerts:,} alerts, ${total_amount:,.2f}"
            )
    finally:
        conn.close()


def report_pipeline_status(**context):
    import redis as redis_lib
    REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
    r = redis_lib.from_url(REDIS_URL, decode_responses=True)
    ti = context["ti"]
    status = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "dag_run_id": context["run_id"],
        "tx_last_5min": ti.xcom_pull(key="tx_5min", task_ids="check_pipeline_flow") or 0,
        "alerts_last_5min": ti.xcom_pull(key="alerts_5min", task_ids="check_pipeline_flow") or 0,
        "fraud_rate_1hr": ti.xcom_pull(key="fraud_rate", task_ids="check_fraud_rate") or 0,
        "containers": {},
    }
    for name in PIPELINE_CONTAINERS:
        task_id = f"ensure_{name.replace('-', '_')}"
        cstatus = ti.xcom_pull(key=f"{name}_status", task_ids=task_id)
        status["containers"][name] = cstatus or {"running": "unknown"}
    r.set("pipeline_health", json.dumps(status), ex=600)
    log.info(f"[status] Health report written to Redis.")


with DAG(
    dag_id="fraud_pipeline_controller",
    description="Monitors and auto-restarts all fraud detection pipeline jobs",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "pipeline", "monitoring"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_infra = PythonOperator(
        task_id="check_infrastructure",
        python_callable=check_infrastructure,
    )

    ensure_generator = PythonOperator(
        task_id="ensure_transaction_generator",
        python_callable=ensure_container_running,
        op_kwargs={"container_name": "transaction-generator", "max_restarts": 3},
    )

    ensure_feature_eng = PythonOperator(
        task_id="ensure_feature_engineering",
        python_callable=ensure_container_running,
        op_kwargs={"container_name": "feature-engineering", "max_restarts": 3},
    )

    ensure_inference = PythonOperator(
        task_id="ensure_fraud_inference",
        python_callable=ensure_container_running,
        op_kwargs={"container_name": "fraud-inference", "max_restarts": 3},
    )

    ensure_alert_svc = PythonOperator(
        task_id="ensure_alert_service",
        python_callable=ensure_container_running,
        op_kwargs={"container_name": "alert-service", "max_restarts": 3},
    )

    check_flow = PythonOperator(
        task_id="check_pipeline_flow",
        python_callable=check_pipeline_flow,
    )

    check_rate = PythonOperator(
        task_id="check_fraud_rate",
        python_callable=check_fraud_rate,
    )

    report_status = PythonOperator(
        task_id="report_pipeline_status",
        python_callable=report_pipeline_status,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    start >> check_infra
    check_infra >> [ensure_generator, ensure_feature_eng, ensure_inference, ensure_alert_svc]
    [ensure_generator, ensure_feature_eng, ensure_inference] >> check_flow
    check_flow >> check_rate
    [check_rate, ensure_alert_svc] >> report_status >> end
