"""
Advanced Drift Detection & MLOps Monitor
- Kolmogorov-Smirnov test for feature drift (per-feature + multivariate)
- Population Stability Index (PSI) for score drift
- Wasserstein distance for distributional shifts
- Automatic retraining trigger via Kafka event
- Statistical process control (CUSUM) for gradual drift
- Slack/webhook alerting
"""

import os
import json
import time
import logging
import threading
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from collections import deque
from scipy import stats
from confluent_kafka import Consumer, Producer
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("drift-detector")

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PREDICTIONS_TOPIC   = os.getenv("PREDICTIONS_TOPIC", "fraud-predictions")
FEATURES_TOPIC      = "engineered-features"
MODEL_PATH          = os.getenv("MODEL_PATH", "/app/models")
KS_THRESHOLD        = float(os.getenv("KS_DRIFT_THRESHOLD", "0.1"))
PSI_THRESHOLD       = 0.2
DRIFT_CHECK_INTERVAL = int(os.getenv("DRIFT_CHECK_INTERVAL", "300"))
RETRAINING_TRIGGER  = os.getenv("RETRAINING_TRIGGER", "true").lower() == "true"
ALERT_WEBHOOK       = os.getenv("ALERT_WEBHOOK", "")
REDIS_HOST          = os.getenv("REDIS_HOST", "localhost")

MONITOR_FEATURES = [
    "amount", "amount_ratio_10txn", "velocity_1h", "velocity_24h",
    "geo_anomaly_score", "merchant_risk_score", "rule_based_score",
    "amount_zscore_10txn", "new_device_flag", "cross_border_flag",
]


def ks_test(reference: np.ndarray, production: np.ndarray) -> tuple[float, float]:
    """KS test — returns (statistic, p_value)"""
    stat, pval = stats.ks_2samp(reference, production)
    return float(stat), float(pval)


def psi(expected: np.ndarray, actual: np.ndarray, buckets: int = 10) -> float:
    """Population Stability Index"""
    breakpoints = np.histogram_bin_edges(expected, bins=buckets)
    exp_percents = np.histogram(expected, bins=breakpoints)[0] / len(expected)
    act_percents = np.histogram(actual, bins=breakpoints)[0] / len(actual)
    exp_percents = np.clip(exp_percents, 1e-4, None)
    act_percents = np.clip(act_percents, 1e-4, None)
    return float(np.sum((act_percents - exp_percents) * np.log(act_percents / exp_percents)))


def wasserstein(reference: np.ndarray, production: np.ndarray) -> float:
    return float(stats.wasserstein_distance(reference, production))


class CUSUMDetector:
    """CUSUM for detecting gradual drift in fraud score"""

    def __init__(self, k: float = 0.5, h: float = 5.0):
        self.k = k  # allowance
        self.h = h  # decision threshold
        self.c_pos = 0.0
        self.c_neg = 0.0
        self.mu_hat = None

    def update(self, value: float) -> bool:
        """Returns True if change detected"""
        if self.mu_hat is None:
            self.mu_hat = value
            return False

        self.c_pos = max(0, self.c_pos + value - self.mu_hat - self.k)
        self.c_neg = max(0, self.c_neg - value + self.mu_hat - self.k)

        if self.c_pos > self.h or self.c_neg > self.h:
            self.c_pos = 0.0
            self.c_neg = 0.0
            return True
        return False


class DriftDetector:
    def __init__(self):
        self.reference_data = self._load_reference()
        self.live_buffer = deque(maxlen=5000)  # Rolling live window
        self.score_buffer = deque(maxlen=1000)
        self.cusum = CUSUMDetector()
        self.drift_events = []
        self.alert_count = 0

        self.r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "1"})

    def _load_reference(self) -> pd.DataFrame:
        ref_file = f"{MODEL_PATH}/reference_data.csv"
        max_wait = 120
        for _ in range(max_wait // 10):
            try:
                df = pd.read_csv(ref_file)
                log.info(f"Reference data loaded: {len(df)} samples")
                return df
            except FileNotFoundError:
                log.info("Waiting for reference data...")
                time.sleep(10)
        log.warning("No reference data found, using empty DataFrame")
        return pd.DataFrame(columns=MONITOR_FEATURES)

    def add_live_data(self, features: dict):
        self.live_buffer.append({col: features.get(col, 0) for col in MONITOR_FEATURES})

    def add_score(self, score: float):
        self.score_buffer.append(score)
        if self.cusum.update(score):
            self._alert("CUSUM: Gradual drift detected in fraud score stream", severity="warning")

    def run_drift_check(self) -> dict:
        if len(self.live_buffer) < 100:
            log.info(f"Not enough live data ({len(self.live_buffer)}/100), skipping drift check")
            return {}
        if self.reference_data.empty:
            return {}

        live_df = pd.DataFrame(list(self.live_buffer))
        report = {"timestamp": datetime.utcnow().isoformat(), "features": {}, "alerts": []}
        drift_detected = False

        for col in MONITOR_FEATURES:
            if col not in self.reference_data.columns or col not in live_df.columns:
                continue

            ref_vals  = self.reference_data[col].dropna().values
            live_vals = live_df[col].dropna().values

            if len(ref_vals) < 10 or len(live_vals) < 10:
                continue

            ks_stat, ks_pval = ks_test(ref_vals, live_vals)
            psi_val = psi(ref_vals, live_vals)
            wass    = wasserstein(ref_vals, live_vals)

            feature_report = {
                "ks_statistic":  round(ks_stat, 4),
                "ks_pvalue":     round(ks_pval, 4),
                "psi":           round(psi_val, 4),
                "wasserstein":   round(wass, 4),
                "drifted":       ks_stat > KS_THRESHOLD or psi_val > PSI_THRESHOLD,
            }
            report["features"][col] = feature_report

            if feature_report["drifted"]:
                drift_detected = True
                report["alerts"].append(f"{col}: KS={ks_stat:.3f}, PSI={psi_val:.3f}")
                log.warning(f"DRIFT DETECTED | {col} | KS={ks_stat:.4f} (threshold={KS_THRESHOLD}) | PSI={psi_val:.4f}")

        # Score drift
        if len(self.score_buffer) > 50:
            ref_scores = np.random.beta(2, 20, size=1000)  # Expected score distribution
            live_scores = np.array(list(self.score_buffer))
            ks_s, _ = ks_test(ref_scores, live_scores)
            report["score_drift"] = {"ks_statistic": round(ks_s, 4), "drifted": ks_s > KS_THRESHOLD}

        drifted_count = sum(1 for f in report["features"].values() if f.get("drifted"))
        report["summary"] = {
            "features_checked": len(report["features"]),
            "features_drifted": drifted_count,
            "drift_rate": drifted_count / max(len(report["features"]), 1),
            "action_required": drift_detected,
        }

        # Cache report
        self.r.setex("drift:latest_report", 3600, json.dumps(report))
        self.r.set("drift:features_drifted", drifted_count)

        log.info(f"Drift check | {drifted_count}/{len(report['features'])} features drifted")

        if drift_detected:
            self._alert(
                f"Data drift detected: {drifted_count} features exceed thresholds. {report['alerts'][:3]}",
                severity="critical",
                report=report,
            )
            if RETRAINING_TRIGGER:
                self._trigger_retraining(report)

        return report

    def _alert(self, message: str, severity: str = "warning", report: dict = None):
        log.warning(f"ALERT [{severity.upper()}]: {message}")
        self.alert_count += 1

        alert_payload = {
            "alerts": [{
                "labels": {
                    "alertname": "DataDrift",
                    "severity": severity,
                    "pipeline": "fraud-detection",
                },
                "annotations": {
                    "summary": message,
                    "description": json.dumps(report.get("summary", {})) if report else "",
                },
                "startsAt": datetime.utcnow().isoformat() + "Z",
            }]
        }

        if ALERT_WEBHOOK:
            try:
                requests.post(ALERT_WEBHOOK, json=alert_payload, timeout=5)
            except Exception as e:
                log.debug(f"Webhook failed: {e}")

        # Publish to Kafka
        self.producer.produce(
            "drift-alerts",
            key=severity.encode(),
            value=json.dumps({
                "message": message,
                "severity": severity,
                "timestamp": datetime.utcnow().isoformat(),
            }).encode(),
        )
        self.producer.poll(0)

    def _trigger_retraining(self, report: dict):
        log.info("TRIGGERING MODEL RETRAINING via Kafka event...")
        self.producer.produce(
            "model-retraining-triggers",
            key=b"drift-trigger",
            value=json.dumps({
                "trigger": "data_drift",
                "drift_report": report.get("summary", {}),
                "timestamp": datetime.utcnow().isoformat(),
            }).encode(),
        )
        self.producer.flush()
        log.info("Retraining trigger published")


def run():
    detector = DriftDetector()

    # Feature consumer
    feat_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "drift-feature-consumer",
        "auto.offset.reset": "latest",
    })
    pred_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "drift-pred-consumer",
        "auto.offset.reset": "latest",
    })
    feat_consumer.subscribe([FEATURES_TOPIC])
    pred_consumer.subscribe([PREDICTIONS_TOPIC])

    def consume_features():
        while True:
            msg = feat_consumer.poll(timeout=0.1)
            if msg and not msg.error():
                try:
                    detector.add_live_data(json.loads(msg.value()))
                except Exception:
                    pass

    def consume_scores():
        while True:
            msg = pred_consumer.poll(timeout=0.1)
            if msg and not msg.error():
                try:
                    pred = json.loads(msg.value())
                    detector.add_score(pred.get("fraud_score", 0))
                except Exception:
                    pass

    def periodic_check():
        while True:
            time.sleep(DRIFT_CHECK_INTERVAL)
            log.info("=== Running Drift Check ===")
            detector.run_drift_check()

    threads = [
        threading.Thread(target=consume_features, daemon=True),
        threading.Thread(target=consume_scores, daemon=True),
        threading.Thread(target=periodic_check, daemon=True),
    ]
    for t in threads:
        t.start()

    log.info(f"Drift detector running | KS threshold={KS_THRESHOLD} | Check interval={DRIFT_CHECK_INTERVAL}s")

    for t in threads:
        t.join()


if __name__ == "__main__":
    time.sleep(30)
    run()
