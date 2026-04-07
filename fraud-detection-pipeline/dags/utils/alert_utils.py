"""
dags/utils/alert_utils.py
Shared utilities for fraud rate alerting and pipeline health checks.
"""

import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import redis

log = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud123@postgres/fraud_detection")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
FRAUD_RATE_THRESHOLD = float(os.getenv("FRAUD_RATE_ALERT_THRESHOLD", "0.05"))


def get_fraud_rate_last_hour() -> dict:
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*)                            AS total_alerts,
                COALESCE(SUM(amount), 0)            AS total_fraud_amount,
                COALESCE(AVG(fraud_probability), 0) AS avg_probability
            FROM fraud_alerts
            WHERE alert_timestamp >= NOW() - INTERVAL '1 hour'
        """)
        alert_row = dict(cur.fetchone())

        cur.execute("""
            SELECT COUNT(*) AS total_tx
            FROM transactions
            WHERE timestamp >= NOW() - INTERVAL '1 hour'
        """)
        tx_row = dict(cur.fetchone())

        total_tx = tx_row["total_tx"] or 0
        total_alerts = alert_row["total_alerts"] or 0
        fraud_rate = total_alerts / total_tx if total_tx > 0 else 0.0

        return {
            "total_tx": total_tx,
            "total_alerts": total_alerts,
            "fraud_rate": round(fraud_rate, 4),
            "total_fraud_amount": float(alert_row["total_fraud_amount"]),
            "avg_probability": float(alert_row["avg_probability"]),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()


def check_fraud_rate_threshold(**context) -> bool:
    stats = get_fraud_rate_last_hour()
    log.info(
        f"[alert] Fraud rate last hour: {stats['fraud_rate']*100:.2f}% "
        f"({stats['total_alerts']:,} alerts / {stats['total_tx']:,} transactions)"
    )
    context["ti"].xcom_push(key="fraud_stats", value=stats)

    if stats["fraud_rate"] > FRAUD_RATE_THRESHOLD:
        msg = (
            f"FRAUD RATE ALERT: {stats['fraud_rate']*100:.2f}% exceeds "
            f"threshold of {FRAUD_RATE_THRESHOLD*100:.1f}% — "
            f"{stats['total_alerts']:,} alerts in last hour, "
            f"total value: ${stats['total_fraud_amount']:,.2f}"
        )
        log.error(msg)
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.lpush("system_alerts", json.dumps({
            "type": "fraud_rate_spike",
            "message": msg,
            "stats": stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))
        r.ltrim("system_alerts", 0, 99)
        raise ValueError(msg)

    log.info(f"[alert] Fraud rate OK: {stats['fraud_rate']*100:.2f}%")
    return True


def check_container_health(container_name: str) -> dict:
    import docker
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        state = container.attrs.get("State", {})
        return {
            "name": container_name,
            "status": state.get("Status", "unknown"),
            "running": state.get("Running", False),
            "restart_count": container.attrs.get("RestartCount", 0),
        }
    except docker.errors.NotFound:
        return {"name": container_name, "status": "not_found", "running": False}
    finally:
        client.close()


def check_pipeline_throughput() -> dict:
    conn = psycopg2.connect(POSTGRES_DSN)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
        """)
        tx_last_5min = cur.fetchone()[0]
        return {
            "tx_last_5min": tx_last_5min,
            "is_healthy": tx_last_5min >= 50,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()
