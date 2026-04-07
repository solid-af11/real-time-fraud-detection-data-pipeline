-- init_db.sql
-- Creates databases and schema for the fraud detection pipeline.
-- Runs automatically on first postgres container start.

-- ── Additional databases ──────────────────────────────────────────────────────
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SELECT 'CREATE DATABASE mlflow'  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec

-- ── Main schema (fraud_detection database) ────────────────────────────────────
\c fraud_detection

-- Raw + enriched transactions
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id          TEXT PRIMARY KEY,
    card_id                 TEXT NOT NULL,
    timestamp               TIMESTAMPTZ NOT NULL,
    amount                  NUMERIC(12, 2) NOT NULL,
    merchant_name           TEXT,
    merchant_category       TEXT,
    merchant_risk_score     NUMERIC(4, 3),
    lat                     NUMERIC(9, 6),
    lon                     NUMERIC(9, 6),
    city                    TEXT,
    country                 TEXT,
    is_fraud                BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_type              TEXT,
    distance_from_home_km   NUMERIC(10, 2),
    transaction_hour        SMALLINT,
    day_of_week             SMALLINT,
    -- Features (written by feature_engineering.py)
    tx_count_5min                   INTEGER,
    tx_count_1hr                    INTEGER,
    avg_amount_1hr                  NUMERIC(12, 2),
    max_amount_1hr                  NUMERIC(12, 2),
    geo_velocity_kmh                NUMERIC(10, 2),
    time_since_last_tx_s            NUMERIC(12, 1),
    high_risk_merchant_count_1hr    INTEGER,
    amount_vs_avg_ratio             NUMERIC(8, 4),
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tx_card_id    ON transactions (card_id);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp  ON transactions (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tx_is_fraud   ON transactions (is_fraud) WHERE is_fraud = TRUE;

-- Fraud alerts (written by fraud_alert_service.py)
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id                  SERIAL PRIMARY KEY,
    transaction_id      TEXT UNIQUE NOT NULL,
    card_id             TEXT NOT NULL,
    timestamp           TIMESTAMPTZ,
    amount              NUMERIC(12, 2),
    merchant_name       TEXT,
    merchant_category   TEXT,
    lat                 NUMERIC(9, 6),
    lon                 NUMERIC(9, 6),
    city                TEXT,
    country             TEXT,
    fraud_probability   NUMERIC(5, 4),
    scoring_method      TEXT,
    is_fraud_actual     BOOLEAN,
    fraud_type_actual   TEXT,
    resolved            BOOLEAN DEFAULT FALSE,
    alert_timestamp     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_card_id        ON fraud_alerts (card_id);
CREATE INDEX IF NOT EXISTS idx_alert_timestamp      ON fraud_alerts (alert_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alert_resolved       ON fraud_alerts (resolved) WHERE resolved = FALSE;

-- Model training audit log
CREATE TABLE IF NOT EXISTS model_training_log (
    id          SERIAL PRIMARY KEY,
    run_date    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dag_run_id  TEXT,
    status      TEXT,
    roc_auc     NUMERIC(5, 4),
    f1          NUMERIC(5, 4),
    notes       TEXT
);

-- Materialised view for fast dashboard queries
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_stats_hourly AS
SELECT
    date_trunc('hour', alert_timestamp)     AS hour,
    COUNT(*)                                AS alert_count,
    SUM(amount)                             AS total_fraud_amount,
    AVG(fraud_probability)                  AS avg_probability,
    COUNT(DISTINCT card_id)                 AS unique_cards
FROM fraud_alerts
GROUP BY 1
ORDER BY 1 DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fraud_stats_hourly ON fraud_stats_hourly (hour);
