-- init_airflow_db.sql
-- Creates the airflow and mlflow databases needed by those services.
-- Runs after 01_fraud.sql on first postgres container start.

SELECT 'CREATE DATABASE airflow OWNER fraud'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE mlflow OWNER fraud'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
  END IF;
END$$;

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO fraud;
