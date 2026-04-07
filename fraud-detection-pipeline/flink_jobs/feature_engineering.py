"""
feature_engineering.py
Flink streaming job — consumes raw transactions from Kafka,
computes real-time features, and writes them to Redis + PostgreSQL.

Features computed per transaction:
  - tx_count_5min      : rolling transaction count in last 5 minutes
  - tx_count_1hr       : rolling transaction count in last 1 hour
  - avg_amount_1hr     : rolling average spend in last 1 hour
  - max_amount_1hr     : max single transaction in last 1 hour
  - geo_velocity_kmh   : speed implied by distance / time since last tx
  - time_since_last_tx : seconds since previous transaction on this card
  - high_risk_merchant_count_1hr : count of high-risk merchants in last 1 hour
  - amount_vs_avg_ratio : current amount / 30-day average (read from Redis)
"""

import json
import math
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import redis
import psycopg2
from kafka import KafkaConsumer, KafkaProducer

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "raw-transactions"
OUTPUT_TOPIC = "enriched-transactions"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://fraud:fraud123@localhost/fraud_detection"
)

HIGH_RISK_THRESHOLD = 0.20  # merchant_risk_score above this = high-risk


# ──────────────────────────────────────────────────────────────────────────────
# In-memory sliding windows (per card)
# ──────────────────────────────────────────────────────────────────────────────

class SlidingWindow:
    """Stores (timestamp, value) tuples; evicts entries older than window_seconds."""

    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds
        self._data: deque[tuple[float, float]] = deque()

    def add(self, ts: float, value: float):
        self._data.append((ts, value))

    def _evict(self, now: float):
        cutoff = now - self.window_seconds
        while self._data and self._data[0][0] < cutoff:
            self._data.popleft()

    def count(self, now: float) -> int:
        self._evict(now)
        return len(self._data)

    def avg(self, now: float) -> float:
        self._evict(now)
        if not self._data:
            return 0.0
        return sum(v for _, v in self._data) / len(self._data)

    def max(self, now: float) -> float:
        self._evict(now)
        if not self._data:
            return 0.0
        return max(v for _, v in self._data)


class CardState:
    def __init__(self):
        self.amounts_5min = SlidingWindow(300)
        self.amounts_1hr = SlidingWindow(3600)
        self.risk_scores_1hr = SlidingWindow(3600)
        self.last_lat: float | None = None
        self.last_lon: float | None = None
        self.last_ts: float | None = None


# ──────────────────────────────────────────────────────────────────────────────
# Geo helper
# ──────────────────────────────────────────────────────────────────────────────

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ──────────────────────────────────────────────────────────────────────────────
# Feature computation
# ──────────────────────────────────────────────────────────────────────────────

def compute_features(tx: dict, state: CardState, r: redis.Redis) -> dict:
    ts = datetime.fromisoformat(tx["timestamp"]).timestamp()
    amount = tx["amount"]
    lat = tx["lat"]
    lon = tx["lon"]
    risk_score = tx["merchant_risk_score"]

    # Update windows
    state.amounts_5min.add(ts, amount)
    state.amounts_1hr.add(ts, amount)
    state.risk_scores_1hr.add(ts, 1.0 if risk_score >= HIGH_RISK_THRESHOLD else 0.0)

    # Geo-velocity
    geo_velocity_kmh = 0.0
    time_since_last_tx = -1.0
    if state.last_lat is not None and state.last_ts is not None:
        dist_km = _haversine_km(state.last_lat, state.last_lon, lat, lon)
        elapsed_h = max((ts - state.last_ts) / 3600.0, 1e-6)
        geo_velocity_kmh = round(dist_km / elapsed_h, 2)
        time_since_last_tx = round(ts - state.last_ts, 1)

    # 30-day rolling average from Redis (persisted across restarts)
    redis_key = f"card:avg_amount:{tx['card_id']}"
    stored = r.get(redis_key)
    rolling_30d_avg = float(stored) if stored else amount
    # Exponential moving average update
    new_avg = rolling_30d_avg * 0.99 + amount * 0.01
    r.set(redis_key, new_avg, ex=86400 * 35)

    amount_vs_avg_ratio = round(amount / rolling_30d_avg, 4) if rolling_30d_avg else 1.0

    # Update state
    state.last_lat = lat
    state.last_lon = lon
    state.last_ts = ts

    features = {
        "tx_count_5min": state.amounts_5min.count(ts),
        "tx_count_1hr": state.amounts_1hr.count(ts),
        "avg_amount_1hr": round(state.amounts_1hr.avg(ts), 2),
        "max_amount_1hr": round(state.amounts_1hr.max(ts), 2),
        "geo_velocity_kmh": geo_velocity_kmh,
        "time_since_last_tx_s": time_since_last_tx,
        "high_risk_merchant_count_1hr": int(state.risk_scores_1hr.count(ts)),
        "amount_vs_avg_ratio": amount_vs_avg_ratio,
        "rolling_30d_avg_amount": round(rolling_30d_avg, 2),
    }

    # Cache enriched features in Redis for the inference layer (TTL = 10 min)
    r.hset(f"features:{tx['card_id']}", mapping={k: str(v) for k, v in features.items()})
    r.expire(f"features:{tx['card_id']}", 600)

    return {**tx, "features": features}


# ──────────────────────────────────────────────────────────────────────────────
# DB sink
# ──────────────────────────────────────────────────────────────────────────────

INSERT_TX_SQL = """
INSERT INTO transactions (
    transaction_id, card_id, timestamp, amount,
    merchant_name, merchant_category, merchant_risk_score,
    lat, lon, city, country,
    is_fraud, fraud_type, distance_from_home_km,
    transaction_hour, day_of_week,
    tx_count_5min, tx_count_1hr, avg_amount_1hr, max_amount_1hr,
    geo_velocity_kmh, time_since_last_tx_s,
    high_risk_merchant_count_1hr, amount_vs_avg_ratio
) VALUES (
    %(transaction_id)s, %(card_id)s, %(timestamp)s, %(amount)s,
    %(merchant_name)s, %(merchant_category)s, %(merchant_risk_score)s,
    %(lat)s, %(lon)s, %(city)s, %(country)s,
    %(is_fraud)s, %(fraud_type)s, %(distance_from_home_km)s,
    %(transaction_hour)s, %(day_of_week)s,
    %(tx_count_5min)s, %(tx_count_1hr)s, %(avg_amount_1hr)s, %(max_amount_1hr)s,
    %(geo_velocity_kmh)s, %(time_since_last_tx_s)s,
    %(high_risk_merchant_count_1hr)s, %(amount_vs_avg_ratio)s
)
ON CONFLICT (transaction_id) DO NOTHING;
"""


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print("[feature-eng] Connecting to Redis…")
    r = redis.from_url(REDIS_URL, decode_responses=True)

    print("[feature-eng] Connecting to PostgreSQL…")
    conn = psycopg2.connect(POSTGRES_DSN)
    conn.autocommit = False
    cur = conn.cursor()

    print("[feature-eng] Connecting to Kafka…")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id="feature-engineering",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=100,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
    )

    card_states: dict[str, CardState] = defaultdict(CardState)
    processed = 0
    batch: list[dict] = []

    print("[feature-eng] Processing stream…")
    for msg in consumer:
        tx = msg.value
        card_id = tx["card_id"]

        try:
            enriched = compute_features(tx, card_states[card_id], r)
        except Exception as exc:
            print(f"[feature-eng] Feature error for {card_id}: {exc}")
            continue

        # Forward to enriched-transactions topic
        producer.send(OUTPUT_TOPIC, key=card_id, value=enriched)

        # Batch DB writes
        flat = {**tx, **enriched["features"]}
        batch.append(flat)
        if len(batch) >= 50:
            try:
                cur.executemany(INSERT_TX_SQL, batch)
                conn.commit()
            except Exception as exc:
                print(f"[feature-eng] DB write error: {exc}")
                conn.rollback()
            batch.clear()

        processed += 1
        if processed % 500 == 0:
            print(f"[feature-eng] Processed {processed:,} transactions")


if __name__ == "__main__":
    main()
