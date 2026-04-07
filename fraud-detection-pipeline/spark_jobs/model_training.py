"""
model_training.py
Spark batch job — reads historical transactions from PostgreSQL,
engineers features, trains a Random Forest fraud classifier,
and registers the model in MLflow.

Triggered daily by Airflow (dags/retraining_dag.py).
"""

import os

import mlflow
import mlflow.sklearn
import numpy as np
from mlflow.models.signature import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_class_weight

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fraud:fraud123@localhost/fraud_detection")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = "fraud-classifier"
EXPERIMENT_NAME = "fraud-detection"

JDBC_URL = "jdbc:postgresql://postgres:5432/fraud_detection"
JDBC_PROPS = {"user": "fraud", "password": "fraud123", "driver": "org.postgresql.Driver"}

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
LABEL_COL = "is_fraud"
TRAINING_DAYS = int(os.getenv("TRAINING_DAYS", "30"))


# ──────────────────────────────────────────────────────────────────────────────
# Spark session
# ──────────────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FraudDetectionTraining")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────

def load_training_data(spark: SparkSession):
    """Load transactions from the last TRAINING_DAYS days."""
    print(f"[training] Loading {TRAINING_DAYS} days of transaction history…")
    df = (
        spark.read.jdbc(JDBC_URL, "transactions", properties=JDBC_PROPS)
        .filter(F.col("timestamp") >= F.date_sub(F.current_date(), TRAINING_DAYS))
        .filter(F.col("time_since_last_tx_s") >= 0)   # drop first-ever transactions
    )

    total = df.count()
    fraud = df.filter(F.col(LABEL_COL) == True).count()
    print(f"[training] Loaded {total:,} transactions | Fraud: {fraud:,} ({fraud/total*100:.2f}%)")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Feature engineering (batch)
# ──────────────────────────────────────────────────────────────────────────────

def engineer_features(df):
    """Add any additional batch-only features."""
    df = df.withColumn(
        "amount_log",
        F.log1p(F.col("amount")),
    ).withColumn(
        "geo_velocity_capped",
        F.least(F.col("geo_velocity_kmh"), F.lit(2000.0)),
    ).withColumn(
        "is_night",
        ((F.col("transaction_hour") >= 23) | (F.col("transaction_hour") <= 5)).cast("int"),
    ).withColumn(
        "is_weekend",
        (F.col("day_of_week") >= 5).cast("int"),
    )
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Model training
# ──────────────────────────────────────────────────────────────────────────────

def train_model(X_train, y_train):
    class_weights = compute_class_weight("balanced", classes=np.array([0, 1]), y=y_train)
    cw = {0: class_weights[0], 1: class_weights[1]}
    print(f"[training] Class weights: {cw}")

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", RandomForestClassifier(
            n_estimators=200,
            max_depth=12,
            min_samples_split=10,
            min_samples_leaf=5,
            max_features="sqrt",
            class_weight=cw,
            n_jobs=-1,
            random_state=42,
        )),
    ])
    pipeline.fit(X_train, y_train)
    return pipeline


# ──────────────────────────────────────────────────────────────────────────────
# Evaluation
# ──────────────────────────────────────────────────────────────────────────────

def evaluate(model, X_test, y_test) -> dict:
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    metrics = {
        "roc_auc": round(roc_auc_score(y_test, y_prob), 4),
        "avg_precision": round(average_precision_score(y_test, y_prob), 4),
        "f1": round(f1_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
    }
    print("[training] Evaluation metrics:")
    for k, v in metrics.items():
        print(f"           {k}: {v}")
    print(classification_report(y_test, y_pred, target_names=["legit", "fraud"]))
    return metrics


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    spark = build_spark()
    df = load_training_data(spark)
    df = engineer_features(df)

    # Collect to pandas for sklearn (dataset fits in driver for up to ~10M rows)
    pandas_df = df.select(FEATURE_COLS + [LABEL_COL]).toPandas()
    pandas_df = pandas_df.fillna(0)

    X = pandas_df[FEATURE_COLS].values.astype(np.float32)
    y = pandas_df[LABEL_COL].values.astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    with mlflow.start_run(run_name="random-forest-retrain"):
        mlflow.log_params({
            "n_estimators": 200,
            "max_depth": 12,
            "training_days": TRAINING_DAYS,
            "train_size": len(X_train),
            "test_size": len(X_test),
        })

        model = train_model(X_train, y_train)
        metrics = evaluate(model, X_test, y_test)
        mlflow.log_metrics(metrics)

        # Log feature importances
        importances = model.named_steps["clf"].feature_importances_
        for feat, imp in zip(FEATURE_COLS, importances):
            mlflow.log_metric(f"importance_{feat}", round(float(imp), 4))

        # Register model
        signature = infer_signature(X_train, model.predict_proba(X_train))
        model_info = mlflow.sklearn.log_model(
            model,
            "model",
            signature=signature,
            registered_model_name=MODEL_NAME,
            input_example=X_train[:5],
        )

        # Promote to Production if AUC > 0.85
        if metrics["roc_auc"] >= 0.85:
            client = mlflow.tracking.MlflowClient()
            latest = client.get_latest_versions(MODEL_NAME, stages=["None"])
            if latest:
                client.transition_model_version_stage(
                    name=MODEL_NAME,
                    version=latest[0].version,
                    stage="Production",
                )
                print(f"[training] Model v{latest[0].version} promoted to Production.")
        else:
            print(f"[training] AUC {metrics['roc_auc']} < 0.85, model NOT promoted.")

    spark.stop()
    print("[training] Done.")


if __name__ == "__main__":
    main()
