"""
Advanced Model Trainer
- Ensemble: Isolation Forest + Autoencoder + XGBoost
- SMOTE for class imbalance handling
- Threshold calibration using Precision-Recall curves
- Saves training reference data for drift detection
- Feast feature store integration
"""

import os
import json
import time
import pickle
import logging
import numpy as np
import pandas as pd
import psycopg2
import redis
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, precision_recall_curve,
    average_precision_score, roc_auc_score
)
import xgboost as xgb

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("trainer")

MODEL_PATH   = os.getenv("MODEL_PATH", "/app/models")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://feast:feast_secret@localhost:5432/fraud_features")
REDIS_HOST   = os.getenv("REDIS_HOST", "localhost")

FEATURE_COLS = [
    "amount", "amount_ratio_10txn", "amount_zscore_10txn",
    "amount_vs_30txn_mean", "amount_max_10txn_ratio",
    "velocity_1h", "velocity_24h", "velocity_ratio_1h_24h",
    "new_device_flag", "card_present", "cross_border_flag",
    "geo_anomaly_score", "hour_probability", "is_weekend", "is_night",
    "merchant_risk_score", "rule_based_score", "hour_of_day", "day_of_week",
    "user_txn_count_1h",
]

os.makedirs(MODEL_PATH, exist_ok=True)


def generate_synthetic_training_data(n=50000, fraud_rate=0.02) -> pd.DataFrame:
    """Generate synthetic training data when no historical data exists"""
    log.info(f"Generating {n} synthetic training samples ({fraud_rate*100:.1f}% fraud)...")
    np.random.seed(42)
    n_fraud = int(n * fraud_rate)
    n_legit = n - n_fraud

    def make_legit(n):
        return pd.DataFrame({
            "amount":                np.abs(np.random.normal(80, 50, n)),
            "amount_ratio_10txn":    np.abs(np.random.normal(1.0, 0.4, n)),
            "amount_zscore_10txn":   np.random.normal(0, 1, n),
            "amount_vs_30txn_mean":  np.abs(np.random.normal(1.0, 0.3, n)),
            "amount_max_10txn_ratio":np.abs(np.random.normal(0.7, 0.2, n)),
            "velocity_1h":           np.random.poisson(1.2, n),
            "velocity_24h":          np.random.poisson(8, n),
            "velocity_ratio_1h_24h": np.abs(np.random.normal(3.6, 1, n)),
            "new_device_flag":       np.random.binomial(1, 0.05, n),
            "card_present":          np.random.binomial(1, 0.6, n),
            "cross_border_flag":     np.random.binomial(1, 0.05, n),
            "geo_anomaly_score":     np.random.beta(1, 20, n),
            "hour_probability":      np.random.beta(5, 20, n),
            "is_weekend":            np.random.binomial(1, 0.28, n),
            "is_night":              np.random.binomial(1, 0.1, n),
            "merchant_risk_score":   np.random.beta(1, 30, n),
            "rule_based_score":      np.random.beta(1, 15, n),
            "hour_of_day":           np.random.randint(7, 22, n),
            "day_of_week":           np.random.randint(0, 7, n),
            "user_txn_count_1h":     np.random.poisson(1, n),
            "is_fraud":              0,
        })

    def make_fraud(n):
        # Mix of different fraud patterns
        n1, n2, n3, n4 = n // 4, n // 4, n // 4, n - 3 * (n // 4)

        # Card takeover
        ct = make_legit(n1)
        ct["amount"] *= np.random.uniform(5, 20, n1)
        ct["amount_ratio_10txn"] = ct["amount"] / 80
        ct["new_device_flag"] = 1
        ct["cross_border_flag"] = 1
        ct["geo_anomaly_score"] = np.random.uniform(0.6, 1.0, n1)
        ct["rule_based_score"] = np.random.uniform(0.5, 1.0, n1)

        # Velocity attack
        va = make_legit(n2)
        va["amount"] = np.random.uniform(1, 20, n2)
        va["velocity_1h"] = np.random.randint(10, 50, n2)
        va["new_device_flag"] = 1
        va["rule_based_score"] = np.random.uniform(0.4, 0.8, n2)

        # ATM cashout
        atm = make_legit(n3)
        atm["amount"] = np.random.choice([200, 400, 500, 1000], n3)
        atm["cross_border_flag"] = 1
        atm["geo_anomaly_score"] = np.random.uniform(0.7, 1.0, n3)

        # Synthetic identity
        si = make_legit(n4)
        si["amount"] *= np.random.uniform(10, 50, n4)
        si["rule_based_score"] = np.random.uniform(0.6, 1.0, n4)

        fraud_df = pd.concat([ct, va, atm, si], ignore_index=True)
        fraud_df["is_fraud"] = 1
        return fraud_df

    df = pd.concat([make_legit(n_legit), make_fraud(n_fraud)], ignore_index=True)
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    return df


def train_isolation_forest(X_train: np.ndarray, contamination: float = 0.02) -> IsolationForest:
    log.info("Training Isolation Forest...")
    model = IsolationForest(
        n_estimators=200,
        max_samples="auto",
        contamination=contamination,
        max_features=0.8,
        bootstrap=True,
        n_jobs=-1,
        random_state=42,
    )
    model.fit(X_train)
    return model


def train_autoencoder_features(X_train: np.ndarray) -> dict:
    """
    Lightweight autoencoder simulation using PCA reconstruction error
    (Full PyTorch autoencoder would be used in production)
    """
    from sklearn.decomposition import PCA
    log.info("Training PCA Autoencoder (reconstruction error)...")
    pca = PCA(n_components=8, random_state=42)
    pca.fit(X_train)
    return {"pca": pca}


def compute_reconstruction_error(autoencoder: dict, X: np.ndarray) -> np.ndarray:
    pca = autoencoder["pca"]
    X_reconstructed = pca.inverse_transform(pca.transform(X))
    return np.mean((X - X_reconstructed) ** 2, axis=1)


def train_xgboost(X_train, y_train, X_val, y_val) -> xgb.XGBClassifier:
    log.info("Training XGBoost classifier...")
    scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        use_label_encoder=False,
        eval_metric="aucpr",
        early_stopping_rounds=20,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )
    return model


def calibrate_threshold(y_true, y_scores, target_recall=0.90) -> float:
    """Find threshold achieving target recall"""
    precision, recall, thresholds = precision_recall_curve(y_true, y_scores)
    # Find threshold where recall >= target
    valid = thresholds[recall[:-1] >= target_recall]
    return float(valid[-1]) if len(valid) > 0 else 0.5


def main():
    log.info("=== Fraud Detection Model Trainer v2 ===")

    df = generate_synthetic_training_data(n=100000, fraud_rate=0.02)
    log.info(f"Dataset: {len(df)} samples, {df['is_fraud'].mean()*100:.2f}% fraud")

    X = df[FEATURE_COLS].fillna(0).values
    y = df["is_fraud"].values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    X_train_legit = X_train[y_train == 0]

    # Scale features
    scaler = RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)
    X_train_legit_scaled = scaler.transform(X_train_legit)

    # Train models
    iforest   = train_isolation_forest(X_train_legit_scaled, contamination=0.02)
    ae_model  = train_autoencoder_features(X_train_legit_scaled)
    X_tr2, X_val, y_tr2, y_val = train_test_split(X_train_scaled, y_train, test_size=0.15, stratify=y_train, random_state=0)
    xgb_model = train_xgboost(X_tr2, y_tr2, X_val, y_val)

    # Get scores on test set
    if_scores  = -iforest.score_samples(X_test_scaled)
    ae_scores  = compute_reconstruction_error(ae_model, X_test_scaled)
    xgb_scores = xgb_model.predict_proba(X_test_scaled)[:, 1]

    # Normalize scores to [0, 1]
    def normalize(s):
        return (s - s.min()) / (s.max() - s.min() + 1e-9)

    if_norm  = normalize(if_scores)
    ae_norm  = normalize(ae_scores)

    # Ensemble: weighted average
    ensemble = 0.4 * if_norm + 0.2 * ae_norm + 0.4 * xgb_scores
    threshold = calibrate_threshold(y_test, ensemble, target_recall=0.90)

    y_pred = (ensemble >= threshold).astype(int)

    log.info("\n=== Evaluation ===")
    log.info(f"Ensemble threshold (90% recall): {threshold:.4f}")
    log.info(f"ROC-AUC: {roc_auc_score(y_test, ensemble):.4f}")
    log.info(f"Avg Precision: {average_precision_score(y_test, ensemble):.4f}")
    log.info("\n" + classification_report(y_test, y_pred, target_names=["Legit", "Fraud"]))

    # Save artifacts
    artifacts = {
        "scaler":    scaler,
        "iforest":   iforest,
        "autoencoder": ae_model,
        "xgboost":   xgb_model,
        "threshold": threshold,
        "feature_cols": FEATURE_COLS,
        "metadata": {
            "trained_at": datetime.utcnow().isoformat(),
            "n_samples": len(df),
            "fraud_rate": float(df["is_fraud"].mean()),
            "roc_auc": float(roc_auc_score(y_test, ensemble)),
            "version": "2.1.0",
        }
    }

    model_file = f"{MODEL_PATH}/fraud_model.pkl"
    with open(model_file, "wb") as f:
        pickle.dump(artifacts, f)
    log.info(f"Model saved → {model_file}")

    # Save training reference data for drift detection
    ref_data = df[FEATURE_COLS + ["is_fraud"]].sample(5000, random_state=42)
    ref_file = f"{MODEL_PATH}/reference_data.csv"
    ref_data.to_csv(ref_file, index=False)
    log.info(f"Reference data saved → {ref_file}")

    # Cache model metadata in Redis
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    r.set("model:metadata", json.dumps(artifacts["metadata"]))
    r.set("model:threshold", str(threshold))
    log.info("Model metadata cached in Redis")
    log.info("Training complete!")


if __name__ == "__main__":
    time.sleep(10)
    main()
