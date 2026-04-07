"""
fraud_alert_service.py
FastAPI service that:
  - Consumes fraud-alerts from Kafka (background thread)
  - Persists alerts to PostgreSQL
  - Exposes REST endpoints for downstream systems
  - Serves real-time stats for Grafana
"""

import asyncio
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import redis
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
ALERTS_TOPIC = os.getenv("FRAUD_ALERTS_TOPIC", "fraud-alerts")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud123@localhost/fraud_detection")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# ──────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ──────────────────────────────────────────────────────────────────────────────

FRAUD_ALERTS_TOTAL = Counter("fraud_alerts_total", "Total fraud alerts generated")
FRAUD_AMOUNT_TOTAL = Counter("fraud_amount_total_usd", "Total USD value of fraud alerts")
FRAUD_BY_CATEGORY = Counter("fraud_by_merchant_category", "Fraud alerts by category", ["category"])
FRAUD_PROBABILITY_HIST = Histogram(
    "fraud_probability",
    "Distribution of fraud probability scores",
    buckets=[0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99, 1.0],
)
ACTIVE_ALERTS_GAUGE = Gauge("active_fraud_alerts", "Unresolved fraud alerts in DB")

# ──────────────────────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────────────────────

INSERT_ALERT_SQL = """
INSERT INTO fraud_alerts (
    transaction_id, card_id, timestamp, amount,
    merchant_name, merchant_category,
    lat, lon, city, country,
    fraud_probability, scoring_method,
    is_fraud_actual, fraud_type_actual,
    alert_timestamp
) VALUES (
    %(transaction_id)s, %(card_id)s, %(timestamp)s, %(amount)s,
    %(merchant_name)s, %(merchant_category)s,
    %(lat)s, %(lon)s, %(city)s, %(country)s,
    %(fraud_probability)s, %(scoring_method)s,
    %(is_fraud_actual)s, %(fraud_type_actual)s,
    %(alert_timestamp)s
)
ON CONFLICT (transaction_id) DO NOTHING;
"""


def get_db_conn():
    return psycopg2.connect(POSTGRES_DSN)


# ──────────────────────────────────────────────────────────────────────────────
# Kafka consumer thread
# ──────────────────────────────────────────────────────────────────────────────

def _kafka_consumer_loop(stop_event: threading.Event):
    """Background thread — consumes fraud-alerts and persists to Postgres."""
    r = redis.from_url(REDIS_URL, decode_responses=True)
    conn = None

    while not stop_event.is_set():
        try:
            consumer = KafkaConsumer(
                ALERTS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                group_id="alert-service",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=2000,
            )
            conn = get_db_conn()
            conn.autocommit = False
            cur = conn.cursor()
            print("[alert-svc] Kafka consumer started.")

            for msg in consumer:
                if stop_event.is_set():
                    break
                alert = msg.value

                # Persist
                try:
                    cur.execute(INSERT_ALERT_SQL, alert)
                    conn.commit()
                except Exception as exc:
                    print(f"[alert-svc] DB insert error: {exc}")
                    conn.rollback()

                # Prometheus
                FRAUD_ALERTS_TOTAL.inc()
                FRAUD_AMOUNT_TOTAL.inc(alert.get("amount", 0))
                FRAUD_BY_CATEGORY.labels(
                    category=alert.get("merchant_category", "unknown")
                ).inc()
                FRAUD_PROBABILITY_HIST.observe(alert.get("fraud_probability", 0))

                # Redis recent-alerts list (for fast dashboard queries)
                r.lpush("recent_alerts", json.dumps(alert))
                r.ltrim("recent_alerts", 0, 999)  # keep last 1000

        except Exception as exc:
            print(f"[alert-svc] Consumer error: {exc}. Retrying in 5s…")
            time.sleep(5)
        finally:
            if conn:
                conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# FastAPI app
# ──────────────────────────────────────────────────────────────────────────────

stop_event = threading.Event()
consumer_thread: Optional[threading.Thread] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_thread
    consumer_thread = threading.Thread(
        target=_kafka_consumer_loop, args=(stop_event,), daemon=True
    )
    consumer_thread.start()
    yield
    stop_event.set()
    if consumer_thread:
        consumer_thread.join(timeout=10)


app = FastAPI(
    title="Fraud Alert Service",
    description="Real-time fraud detection alerts API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

r_client = redis.from_url(REDIS_URL, decode_responses=True)


# ──────────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/metrics")
def metrics():
    """Prometheus scrape endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/alerts/recent")
def get_recent_alerts(limit: int = Query(default=20, le=200)):
    """Return the most recent N fraud alerts (from Redis cache)."""
    raw = r_client.lrange("recent_alerts", 0, limit - 1)
    return [json.loads(a) for a in raw]


@app.get("/alerts/{transaction_id}")
def get_alert(transaction_id: str):
    """Fetch a specific alert by transaction ID."""
    conn = get_db_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM fraud_alerts WHERE transaction_id = %s", (transaction_id,)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")
        return dict(row)
    finally:
        conn.close()


@app.get("/alerts/card/{card_id}")
def get_alerts_by_card(card_id: str, limit: int = Query(default=10, le=50)):
    """Return recent alerts for a specific card."""
    conn = get_db_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT * FROM fraud_alerts
            WHERE card_id = %s
            ORDER BY alert_timestamp DESC
            LIMIT %s
            """,
            (card_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


@app.get("/stats/summary")
def get_stats():
    """Return aggregate fraud stats for the Grafana dashboard."""
    conn = get_db_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) AS total_alerts,
                COALESCE(SUM(amount), 0) AS total_fraud_amount,
                COALESCE(AVG(fraud_probability), 0) AS avg_fraud_probability,
                COUNT(DISTINCT card_id) AS unique_cards_flagged
            FROM fraud_alerts
            WHERE alert_timestamp >= NOW() - INTERVAL '24 hours'
        """)
        row = dict(cur.fetchone())

        cur.execute("""
            SELECT merchant_category, COUNT(*) AS count
            FROM fraud_alerts
            WHERE alert_timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY merchant_category
            ORDER BY count DESC
            LIMIT 10
        """)
        row["top_categories"] = [dict(r) for r in cur.fetchall()]
        return row
    finally:
        conn.close()


@app.get("/stats/timeseries")
def get_timeseries(
    bucket_minutes: int = Query(default=5, le=60),
    hours: int = Query(default=6, le=48),
):
    """Return fraud alert counts bucketed by time (for Grafana time-series panels)."""
    conn = get_db_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                date_trunc('minute', alert_timestamp) -
                  (EXTRACT(MINUTE FROM alert_timestamp)::int %% %s) * INTERVAL '1 minute'
                  AS bucket,
                COUNT(*) AS alert_count,
                COALESCE(SUM(amount), 0) AS fraud_amount
            FROM fraud_alerts
            WHERE alert_timestamp >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY bucket
            ORDER BY bucket
            """,
            (bucket_minutes, hours),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fraud_alert_service:app", host="0.0.0.0", port=8000, reload=False)
