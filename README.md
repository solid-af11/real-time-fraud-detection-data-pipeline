# 🔍 Real-Time Fraud Detection Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)

**End-to-end streaming ML pipeline that detects credit card fraud in real time**

[Architecture](#architecture) · [Features](#features) · [Quickstart](#quickstart) · [Tech Stack](#tech-stack) · [Screenshots](#screenshots) · [API Reference](#api-reference)

</div>

---

## Overview

A production-grade fraud detection system that processes **50 transactions per second**, engineers features on a live stream using sliding windows, scores every transaction with a machine learning model, and surfaces alerts through a REST API — all orchestrated by Apache Airflow.

### Key Numbers

| Metric | Value |
|---|---|
| Throughput | 50 transactions / second |
| Simulated fraud rate | 2% (~1 fraud/sec) |
| Feature window | 5 min + 1 hour rolling |
| Model retraining | Daily at 02:00 UTC |
| Health check interval | Every 5 minutes |
| Alert latency | < 500ms end-to-end |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Airflow (Orchestration)                │
│         fraud_pipeline_controller  │  fraud_model_retraining     │
└──────────────────┬──────────────────────────────────────────────┘
                   │ monitors & auto-restarts
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│   Transaction Generator (Python Faker)                           │
│   50 tx/sec · 4 fraud patterns · 500 cardholder profiles        │
│                        │                                         │
│                        ▼                                         │
│              ┌─────────────────┐                                 │
│              │  Apache Kafka   │  raw-transactions topic         │
│              └────────┬────────┘                                 │
│                       │                                          │
│                       ▼                                          │
│         ┌─────────────────────────┐                              │
│         │   Feature Engineering   │  Sliding windows             │
│         │   (Flink-style Python)  │  Geo-velocity                │
│         └──────┬──────────────────┘  Risk scoring               │
│                │                                                 │
│         ┌──────┴──────┐                                          │
│         ▼             ▼                                          │
│      Redis          PostgreSQL                                   │
│   (feature store)  (event history)                               │
│         │             │                                          │
│         ▼             ▼                                          │
│   ┌──────────┐   ┌──────────┐                                    │
│   │  Flink   │   │  Spark   │  Daily batch training             │
│   │Inference │   │Training  │  MLflow model registry            │
│   └────┬─────┘   └──────────┘                                    │
│        │                                                         │
│        ▼                                                         │
│   fraud-alerts (Kafka topic)                                     │
│        │                                                         │
│        ▼                                                         │
│   FastAPI Alert Service          Grafana + Prometheus            │
│   REST endpoints                 Live monitoring dashboard       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Features

### Fraud Patterns Simulated

| Pattern | Description | Share |
|---|---|---|
| `geo_jump` | Transaction far from home / last known location | ~35% of fraud |
| `velocity_burst` | Many small transactions in rapid succession | ~30% of fraud |
| `high_amount` | Single transaction near the card's credit limit | ~25% of fraud |
| `card_testing` | Tiny amounts to test a stolen card's validity | ~10% of fraud |

### Features Engineered in Real Time

| Feature | Window | Description |
|---|---|---|
| `tx_count_5min` | 5 min | Transaction count per card |
| `tx_count_1hr` | 1 hour | Transaction count per card |
| `avg_amount_1hr` | 1 hour | Average spend per card |
| `max_amount_1hr` | 1 hour | Largest transaction per card |
| `geo_velocity_kmh` | since last tx | Implied travel speed |
| `time_since_last_tx_s` | since last tx | Seconds since previous tx |
| `high_risk_merchant_count_1hr` | 1 hour | High-risk merchant hits |
| `amount_vs_avg_ratio` | 30 days | Current vs rolling average |

### Airflow Orchestration

- **`fraud_pipeline_controller`** — runs every 5 minutes, checks all containers are running, verifies pipeline throughput, alerts if fraud rate exceeds 5%, auto-restarts failed containers up to 3 times
- **`fraud_model_retraining`** — runs daily at 02:00 UTC, validates data quality, triggers Spark training, registers model in MLflow, signals inference layer to reload

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Event generation | Python + Faker | Synthetic transaction stream |
| Message broker | Apache Kafka | Real-time event streaming |
| Stream processing | Python (Flink-style) | Sliding window feature engineering |
| Feature store | Redis | Low-latency feature serving |
| Batch processing | Apache Spark | Historical ML training |
| ML tracking | MLflow | Experiment tracking + model registry |
| ML model | Scikit-learn Random Forest | Fraud classification |
| Data warehouse | PostgreSQL | Raw + enriched event storage |
| API layer | FastAPI | Fraud alert REST endpoints |
| Monitoring | Grafana + Prometheus | Live pipeline dashboard |
| Orchestration | Apache Airflow | Pipeline + retraining automation |
| Containerisation | Docker Compose | Full local deployment |

---

## Quickstart

### Prerequisites

- Docker Desktop (8 GB RAM recommended)
- Docker Compose v2
- Python 3.11+

### 1. Clone and set up

```bash
git clone https://github.com/YOUR_USERNAME/fraud-detection-pipeline
cd fraud-detection-pipeline
python3 -m venv venv && source venv/bin/activate
pip install kafka-python redis scikit-learn numpy psycopg2-binary faker mlflow-skinny fastapi uvicorn prometheus-client
```

### 2. Start infrastructure

```bash
# Free up Docker disk space first
docker system prune -a --volumes -f

# Start core services
docker compose up -d zookeeper kafka redis postgres
```

Wait 30 seconds, then verify:
```bash
docker compose ps   # all four should show (healthy)
```

### 3. Start Airflow

```bash
# Fix Postgres permissions
docker exec -it postgres psql -U fraud -d airflow -c "GRANT ALL ON SCHEMA public TO airflow;"

# Initialise and start Airflow
docker compose up -d airflow-init
docker compose logs -f airflow-init   # wait for "Airflow initialised successfully."
docker compose up -d airflow-webserver airflow-scheduler
```

### 4. Run the pipeline (4 terminals)

```bash
# Terminal 1 — Transaction generator
cd ~/fraud-detection-pipeline && source venv/bin/activate
python generator/transaction_producer.py

# Terminal 2 — Feature engineering
cd ~/fraud-detection-pipeline && source venv/bin/activate
python flink_jobs/feature_engineering.py

# Terminal 3 — Fraud inference
cd ~/fraud-detection-pipeline && source venv/bin/activate
python flink_jobs/fraud_inference.py

# Terminal 4 — Alert API
cd ~/fraud-detection-pipeline/api && source ../venv/bin/activate
uvicorn fraud_alert_service:app --host 0.0.0.0 --port 8000
```

### 5. Enable Airflow DAGs

Open http://localhost:8082 (admin / admin) and toggle both DAGs on.

### 6. Verify

```bash
# Live fraud alerts
curl http://localhost:8000/alerts/recent | python3 -m json.tool

# Pipeline health (written by Airflow every 5 min)
curl http://localhost:8000/pipeline/health | python3 -m json.tool

# Stats summary
curl http://localhost:8000/stats/summary | python3 -m json.tool
```

---

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/alerts/recent` | GET | Last 20 fraud alerts |
| `/alerts/{transaction_id}` | GET | Single alert by ID |
| `/alerts/card/{card_id}` | GET | Alerts for a specific card |
| `/stats/summary` | GET | 24-hour fraud statistics |
| `/stats/timeseries` | GET | Bucketed time-series data |
| `/pipeline/health` | GET | Airflow pipeline health report |
| `/pipeline/alerts` | GET | System-level alerts |
| `/metrics` | GET | Prometheus metrics |
| `/docs` | GET | Interactive Swagger UI |

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| FastAPI docs | http://localhost:8000/docs | — |
| Airflow | http://localhost:8082 | admin / admin |
| Grafana | http://localhost:3000 | admin / fraud123 |
| MLflow | http://localhost:5000 | — |
| Kafka UI | http://localhost:8080 | — |

---

## Screenshots

> Add screenshots of your running pipeline here:
> - Airflow DAG graph view
> - Grafana dashboard
> - FastAPI Swagger UI
> - Kafka UI showing topic throughput

---

## Project Structure

```
fraud-detection-pipeline/
├── generator/
│   ├── transaction_producer.py    # Kafka producer with fraud simulation
│   └── Dockerfile
├── flink_jobs/
│   ├── feature_engineering.py    # Sliding window feature computation
│   ├── fraud_inference.py        # Real-time ML scoring
│   └── Dockerfile
├── spark_jobs/
│   └── model_training.py         # Batch Random Forest training
├── api/
│   ├── fraud_alert_service.py    # FastAPI + Prometheus
│   └── Dockerfile
├── dags/
│   ├── fraud_pipeline_controller.py   # 5-min health monitor DAG
│   ├── fraud_model_retraining.py      # Daily retraining DAG
│   └── utils/
│       └── alert_utils.py
├── config/
│   ├── init_db.sql               # PostgreSQL schema
│   └── init_airflow_db.sql       # Airflow + MLflow DB setup
├── monitoring/
│   └── prometheus.yml
├── docker-compose.yml
└── README.md
```

---

## License

MIT — feel free to use this project as a reference or starting point.

---

<div align="center">
Built with ❤️
</div>
