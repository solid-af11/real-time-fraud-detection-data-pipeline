"""
fraud_inference.py
Flink inference layer — reads enriched transactions, loads the latest
Random Forest model from MLflow, scores in real time, and publishes
fraud alerts to the fraud-alerts Kafka topic.
"""

import json
import os
import pickle
import time
from datetime import datetime, timezone

import numpy as np
import redis
import mlflow
from kafka import KafkaConsumer, KafkaProducer

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "enriched-transactions"
ALERTS_TOPIC = "fraud-alerts"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "")
MODEL_NAME = "fraud-classifier"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.50"))
MODEL_RELOAD_INTERVAL_S = 999999   # reload model from MLflow every 5 minutes

# Feature columns expected by the model (must match spark_jobs/model_training.py)
FEATURE_COLS = [
    "amount",
    "merchant_risk_score",
    "distance_from_home_km",
    "transaction_hour",
    "day_of_week",
    "tx_count_5min",
    "tx_count_1hr",
    "avg_amount_1hr",
    "max_amount_1hr",
    "geo_velocity_kmh",
    "time_since_last_tx_s",
    "high_risk_merchant_count_1hr",
    "amount_vs_avg_ratio",
]


# ──────────────────────────────────────────────────────────────────────────────
# Model loader
# ──────────────────────────────────────────────────────────────────────────────

class ModelRegistry:
    def __init__(self):
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        self._model = None
        self._version = None
        self._last_loaded = 0.0

    def _try_load(self):
        """Attempt to load the latest Production model from MLflow."""
        try:
            client = mlflow.tracking.MlflowClient()
            versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
            if not versions:
                # Fall back to latest version regardless of stage
                versions = client.get_latest_versions(MODEL_NAME)
            if not versions:
                print("[inference] No model found in MLflow yet, using heuristic fallback.")
                return None, None
            latest = versions[0]
            model_uri = f"models:/{MODEL_NAME}/{latest.version}"
            model = mlflow.sklearn.load_model(model_uri)
            print(f"[inference] Loaded model version {latest.version} from MLflow.")
            return model, latest.version
        except Exception as exc:
            print(f"[inference] Could not load model from MLflow: {exc}")
            return None, None

    def get_model(self):
        now = time.monotonic()
        if now - self._last_loaded > MODEL_RELOAD_INTERVAL_S:
            model, version = self._try_load()
            if model is not None:
                self._model = model
                self._version = version
            self._last_loaded = now
        return self._model, self._version


# ──────────────────────────────────────────────────────────────────────────────
# Heuristic fallback (used before first model is trained)
# ──────────────────────────────────────────────────────────────────────────────

def _heuristic_score(features: dict) -> float:
    """
    Simple rule-based fraud score when no ML model is available.
    Returns a probability-like float in [0, 1].
    """
    score = 0.0

    if features.get("geo_velocity_kmh", 0) > 500:
        score += 0.40
    if features.get("tx_count_5min", 0) >= 5:
        score += 0.25
    if features.get("amount_vs_avg_ratio", 1) > 5.0:
        score += 0.20
    if features.get("merchant_risk_score", 0) >= 0.35:
        score += 0.15
    if features.get("time_since_last_tx_s", 9999) < 30 and features.get("tx_count_5min", 0) > 3:
        score += 0.20

    return min(score, 1.0)


# ──────────────────────────────────────────────────────────────────────────────
# Scoring
# ──────────────────────────────────────────────────────────────────────────────

def score_transaction(tx: dict, model_registry: ModelRegistry) -> dict:
    features = tx.get("features", {})
    model, model_version = model_registry.get_model()

    if model is not None:
        try:
            X = np.array([[features.get(col, 0.0) for col in FEATURE_COLS]])
            fraud_prob = float(model.predict_proba(X)[0][1])
            scoring_method = f"mlflow_v{model_version}"
        except Exception as exc:
            print(f"[inference] Model prediction error: {exc}, falling back to heuristic.")
            fraud_prob = _heuristic_score(features)
            scoring_method = "heuristic_fallback"
    else:
        fraud_prob = _heuristic_score(features)
        scoring_method = "heuristic_fallback"

    is_fraud_predicted = fraud_prob >= FRAUD_THRESHOLD

    return {
        "transaction_id": tx["transaction_id"],
        "card_id": tx["card_id"],
        "timestamp": tx["timestamp"],
        "amount": tx["amount"],
        "merchant_name": tx["merchant_name"],
        "merchant_category": tx["merchant_category"],
        "lat": tx["lat"],
        "lon": tx["lon"],
        "city": tx["city"],
        "country": tx["country"],
        "fraud_probability": round(fraud_prob, 4),
        "is_fraud_predicted": is_fraud_predicted,
        "is_fraud_actual": tx.get("is_fraud", False),   # ground truth (simulation only)
        "fraud_type_actual": tx.get("fraud_type"),
        "scoring_method": scoring_method,
        "features": features,
        "alert_timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    model_registry = ModelRegistry()

    r = redis.from_url(REDIS_URL, decode_responses=True)

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id="fraud-inference",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    scored = fraud_count = 0
    print(f"[inference] Scoring transactions (threshold={FRAUD_THRESHOLD})…")

    for msg in consumer:
        tx = msg.value
        try:
            result = score_transaction(tx, model_registry)
        except Exception as exc:
            print(f"[inference] Scoring error: {exc}")
            continue

        # Always publish full result to enriched-scored topic
        producer.send("scored-transactions", key=tx["card_id"], value=result)

        # Publish fraud alerts to dedicated topic
        if result["is_fraud_predicted"]:
            producer.send(ALERTS_TOPIC, key=tx["card_id"], value=result)
            r.lpush(f"alerts:{tx['card_id']}", json.dumps(result))
            r.ltrim(f"alerts:{tx['card_id']}", 0, 99)   # keep last 100 alerts per card
            fraud_count += 1

        scored += 1
        if scored % 1000 == 0:
            precision_approx = fraud_count / scored * 100
            print(
                f"[inference] Scored {scored:,} | "
                f"Alerts: {fraud_count:,} ({precision_approx:.2f}%)"
            )


if __name__ == "__main__":
    main()
