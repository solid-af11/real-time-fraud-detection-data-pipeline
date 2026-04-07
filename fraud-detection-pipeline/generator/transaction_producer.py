"""
transaction_producer.py
Generates synthetic credit card transactions and publishes them to Kafka.

Features:
  - Realistic cardholder profiles (home lat/lon, spending habits)
  - Merchant categories with risk scores
  - Geo-coordinates per transaction
  - ~2% injected fraud patterns (velocity burst, geo-jump, high-amount)
"""

import json
import os
import random
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-transactions")
TPS = float(os.getenv("TRANSACTIONS_PER_SECOND", "50"))
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.02"))

fake = Faker()
Faker.seed(42)
random.seed(42)

# ──────────────────────────────────────────────────────────────────────────────
# Static reference data
# ──────────────────────────────────────────────────────────────────────────────

MERCHANT_CATEGORIES = {
    "grocery":          {"risk_score": 0.05, "avg_amount": 85,   "std": 40},
    "gas_station":      {"risk_score": 0.08, "avg_amount": 55,   "std": 20},
    "restaurant":       {"risk_score": 0.06, "avg_amount": 45,   "std": 25},
    "online_retail":    {"risk_score": 0.18, "avg_amount": 120,  "std": 90},
    "electronics":      {"risk_score": 0.22, "avg_amount": 350,  "std": 280},
    "travel":           {"risk_score": 0.15, "avg_amount": 480,  "std": 400},
    "entertainment":    {"risk_score": 0.10, "avg_amount": 60,   "std": 35},
    "pharmacy":         {"risk_score": 0.07, "avg_amount": 40,   "std": 20},
    "atm_withdrawal":   {"risk_score": 0.25, "avg_amount": 200,  "std": 100},
    "gambling":         {"risk_score": 0.40, "avg_amount": 300,  "std": 250},
    "luxury_goods":     {"risk_score": 0.30, "avg_amount": 800,  "std": 600},
    "crypto_exchange":  {"risk_score": 0.45, "avg_amount": 500,  "std": 400},
}

CATEGORY_WEIGHTS = [0.20, 0.10, 0.15, 0.15, 0.05, 0.05, 0.08, 0.08, 0.04, 0.02, 0.04, 0.04]

# ──────────────────────────────────────────────────────────────────────────────
# Data models
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class CardholderProfile:
    card_id: str
    name: str
    home_lat: float
    home_lon: float
    credit_limit: float
    avg_monthly_spend: float
    preferred_categories: list[str]
    last_transaction_lat: Optional[float] = None
    last_transaction_lon: Optional[float] = None
    last_transaction_ts: Optional[float] = None


@dataclass
class Transaction:
    transaction_id: str
    card_id: str
    timestamp: str
    amount: float
    merchant_name: str
    merchant_category: str
    merchant_risk_score: float
    lat: float
    lon: float
    city: str
    country: str
    is_fraud: bool
    fraud_type: Optional[str]   # None | "velocity_burst" | "geo_jump" | "high_amount" | "card_testing"
    distance_from_home_km: float
    transaction_hour: int
    day_of_week: int


# ──────────────────────────────────────────────────────────────────────────────
# Profile factory
# ──────────────────────────────────────────────────────────────────────────────

def _generate_profiles(n: int = 500) -> list[CardholderProfile]:
    profiles = []
    categories = list(MERCHANT_CATEGORIES.keys())
    for _ in range(n):
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        profiles.append(CardholderProfile(
            card_id=f"CARD-{uuid.uuid4().hex[:12].upper()}",
            name=fake.name(),
            home_lat=lat,
            home_lon=lon,
            credit_limit=random.choice([1000, 2000, 5000, 10000, 25000]),
            avg_monthly_spend=random.uniform(500, 4000),
            preferred_categories=random.sample(categories, k=random.randint(3, 6)),
        ))
    return profiles


# ──────────────────────────────────────────────────────────────────────────────
# Geo helpers
# ──────────────────────────────────────────────────────────────────────────────

import math

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _nearby_coords(lat: float, lon: float, radius_km: float = 30) -> tuple[float, float]:
    """Return a random point within radius_km of the given coordinate."""
    radius_deg = radius_km / 111.0
    dlat = random.uniform(-radius_deg, radius_deg)
    dlon = random.uniform(-radius_deg, radius_deg)
    return round(lat + dlat, 6), round(lon + dlon, 6)


def _far_coords() -> tuple[float, float]:
    """Return a random point anywhere on earth (fraud geo-jump)."""
    return round(float(fake.latitude()), 6), round(float(fake.longitude()), 6)


# ──────────────────────────────────────────────────────────────────────────────
# Transaction factory
# ──────────────────────────────────────────────────────────────────────────────

def _make_transaction(profile: CardholderProfile, inject_fraud: bool = False) -> Transaction:
    now = datetime.now(timezone.utc)
    categories = list(MERCHANT_CATEGORIES.keys())
    fraud_type: Optional[str] = None

    # Pick category (bias toward preferred)
    if random.random() < 0.70 and profile.preferred_categories:
        category = random.choice(profile.preferred_categories)
    else:
        category = random.choices(categories, weights=CATEGORY_WEIGHTS)[0]

    cat_meta = MERCHANT_CATEGORIES[category]

    # Base amount
    amount = max(1.0, round(random.gauss(cat_meta["avg_amount"], cat_meta["std"]), 2))

    # Location
    lat, lon = _nearby_coords(profile.home_lat, profile.home_lon, radius_km=50)

    # ── Inject fraud patterns ──────────────────────────────────────────────
    if inject_fraud:
        pattern = random.choices(
            ["velocity_burst", "geo_jump", "high_amount", "card_testing"],
            weights=[0.30, 0.35, 0.25, 0.10],
        )[0]
        fraud_type = pattern

        if pattern == "velocity_burst":
            # Small amounts, rapid succession — simulate repeated hits
            amount = round(random.uniform(1.0, 15.0), 2)
            category = "online_retail"

        elif pattern == "geo_jump":
            # Transaction far from home and last known location
            lat, lon = _far_coords()
            category = random.choice(["atm_withdrawal", "electronics", "luxury_goods"])
            amount = round(random.uniform(200, 1500), 2)

        elif pattern == "high_amount":
            # Unusually large single transaction
            amount = round(profile.credit_limit * random.uniform(0.6, 0.95), 2)
            category = random.choice(["electronics", "luxury_goods", "crypto_exchange"])

        elif pattern == "card_testing":
            # Tiny amounts across multiple merchants (testing stolen card)
            amount = round(random.uniform(0.50, 2.00), 2)
            category = random.choice(["online_retail", "entertainment"])

    distance_km = _haversine_km(profile.home_lat, profile.home_lon, lat, lon)

    # Update profile's last-known location
    profile.last_transaction_lat = lat
    profile.last_transaction_lon = lon
    profile.last_transaction_ts = now.timestamp()

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        card_id=profile.card_id,
        timestamp=now.isoformat(),
        amount=amount,
        merchant_name=fake.company(),
        merchant_category=category,
        merchant_risk_score=cat_meta["risk_score"],
        lat=lat,
        lon=lon,
        city=fake.city(),
        country=fake.country_code(),
        is_fraud=inject_fraud,
        fraud_type=fraud_type,
        distance_from_home_km=round(distance_km, 2),
        transaction_hour=now.hour,
        day_of_week=now.weekday(),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Kafka helpers
# ──────────────────────────────────────────────────────────────────────────────

def _build_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                compression_type="gzip",
                batch_size=16384,
                linger_ms=5,
            )
            print(f"[producer] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/{retries}). Retrying in {delay}s…")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def _on_send_error(exc):
    print(f"[producer] ERROR sending message: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print("[producer] Generating cardholder profiles…")
    profiles = _generate_profiles(500)
    producer = _build_producer()

    interval = 1.0 / TPS
    sent = 0
    fraud_sent = 0

    print(f"[producer] Streaming {TPS} tx/s to topic '{KAFKA_TOPIC}' (fraud rate: {FRAUD_RATE*100:.1f}%)")

    while True:
        loop_start = time.monotonic()

        profile = random.choice(profiles)
        inject_fraud = random.random() < FRAUD_RATE
        tx = _make_transaction(profile, inject_fraud=inject_fraud)

        payload = asdict(tx)
        producer.send(
            KAFKA_TOPIC,
            key=tx.card_id,
            value=payload,
        ).add_errback(_on_send_error)

        sent += 1
        if tx.is_fraud:
            fraud_sent += 1

        if sent % 1000 == 0:
            actual_rate = fraud_sent / sent * 100
            print(
                f"[producer] Sent {sent:,} transactions | "
                f"Fraud: {fraud_sent:,} ({actual_rate:.2f}%)"
            )

        elapsed = time.monotonic() - loop_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
