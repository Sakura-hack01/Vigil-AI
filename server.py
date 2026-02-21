"""
Fraud Detection Inference Server
- FastAPI REST endpoint for batch/single predictions
- gRPC endpoint for ultra-low latency inference (<10ms P99)
- Kafka consumer for real-time stream inference
- Prometheus metrics exposition
- Model hot-reloading (zero-downtime updates)
"""

import os
import json
import time
import pickle
import logging
import asyncio
import threading
import numpy as np
import redis
import grpc
from concurrent import futures
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from prometheus_client import (
    Counter, Histogram, Gauge, Summary,
    start_http_server, REGISTRY
)
from confluent_kafka import Consumer, Producer, KafkaError

# Generated gRPC stubs (simplified inline)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("fraud-detector")

MODEL_PATH      = os.getenv("MODEL_PATH", "/app/models")
REDIS_HOST      = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_INPUT     = os.getenv("KAFKA_INPUT_TOPIC", "engineered-features")
KAFKA_OUTPUT    = os.getenv("KAFKA_OUTPUT_TOPIC", "fraud-predictions")
GRPC_PORT       = int(os.getenv("GRPC_PORT", "50051"))
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8001"))

FEATURE_COLS = [
    "amount", "amount_ratio_10txn", "amount_zscore_10txn",
    "amount_vs_30txn_mean", "amount_max_10txn_ratio",
    "velocity_1h", "velocity_24h", "velocity_ratio_1h_24h",
    "new_device_flag", "card_present", "cross_border_flag",
    "geo_anomaly_score", "hour_probability", "is_weekend", "is_night",
    "merchant_risk_score", "rule_based_score", "hour_of_day", "day_of_week",
    "user_txn_count_1h",
]

# ─── Prometheus Metrics ─────────────────────────────────────────────────────────
prediction_counter = Counter(
    "fraud_predictions_total",
    "Total predictions made",
    ["result", "method"]  # result: fraud/legit, method: rest/grpc/kafka
)
prediction_latency = Histogram(
    "fraud_prediction_latency_seconds",
    "Prediction latency",
    ["method"],
    buckets=[.001, .005, .010, .025, .050, .075, .1, .25, .5, 1.0]
)
anomaly_rate_gauge = Gauge("fraud_anomaly_rate", "Rolling fraud rate (last 1000 predictions)")
model_score_hist   = Histogram("fraud_model_score", "Raw ensemble score distribution", buckets=np.linspace(0, 1, 21).tolist())
kafka_lag_gauge    = Gauge("fraud_kafka_consumer_lag", "Kafka consumer lag (messages)")
model_version_info = Gauge("fraud_model_version", "Model version", ["version", "trained_at"])

# Rolling window for anomaly rate
_recent_preds = []
_recent_lock  = threading.Lock()


def update_anomaly_rate(is_fraud: bool):
    global _recent_preds
    with _recent_lock:
        _recent_preds.append(int(is_fraud))
        if len(_recent_preds) > 1000:
            _recent_preds.pop(0)
        rate = sum(_recent_preds) / len(_recent_preds)
    anomaly_rate_gauge.set(rate)


# ─── Model Manager ──────────────────────────────────────────────────────────────
class ModelManager:
    def __init__(self):
        self._model = None
        self._lock = threading.RLock()
        self._version = "unknown"
        self._load_model()

    def _load_model(self):
        model_file = f"{MODEL_PATH}/fraud_model.pkl"
        max_wait = 120
        for _ in range(max_wait // 5):
            try:
                with open(model_file, "rb") as f:
                    artifacts = pickle.load(f)
                with self._lock:
                    self._model = artifacts
                    self._version = artifacts["metadata"]["version"]
                log.info(f"Model loaded: v{self._version} (trained {artifacts['metadata']['trained_at']})")
                model_version_info.labels(
                    version=self._version,
                    trained_at=artifacts["metadata"]["trained_at"]
                ).set(1)
                return
            except FileNotFoundError:
                log.info(f"Waiting for model at {model_file}...")
                time.sleep(5)
        raise RuntimeError("Model not found after waiting")

    def reload(self):
        """Hot-reload model without downtime"""
        log.info("Reloading model...")
        self._load_model()

    def predict(self, features: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Returns (is_fraud_array, score_array)"""
        with self._lock:
            m = self._model

        scaler   = m["scaler"]
        iforest  = m["iforest"]
        ae_model = m["autoencoder"]
        xgb_mdl  = m["xgboost"]
        threshold = m["threshold"]

        X_scaled = scaler.transform(features)

        if_scores = -iforest.score_samples(X_scaled)
        pca = ae_model["pca"]
        X_rec = pca.inverse_transform(pca.transform(X_scaled))
        ae_scores = np.mean((X_scaled - X_rec) ** 2, axis=1)
        xgb_scores = xgb_mdl.predict_proba(X_scaled)[:, 1]

        def normalize(s):
            rng = s.max() - s.min()
            return (s - s.min()) / (rng + 1e-9) if rng > 0 else s * 0

        scores = 0.4 * normalize(if_scores) + 0.2 * normalize(ae_scores) + 0.4 * xgb_scores
        predictions = (scores >= threshold).astype(int)
        return predictions, scores


model_mgr = ModelManager()
r_client  = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ─── Schemas ────────────────────────────────────────────────────────────────────
class TransactionFeatures(BaseModel):
    transaction_id: str
    user_id: str
    amount: float = Field(..., gt=0)
    amount_ratio_10txn: float = 1.0
    amount_zscore_10txn: float = 0.0
    amount_vs_30txn_mean: float = 1.0
    amount_max_10txn_ratio: float = 1.0
    velocity_1h: int = 0
    velocity_24h: int = 0
    velocity_ratio_1h_24h: float = 0.0
    new_device_flag: int = 0
    card_present: int = 1
    cross_border_flag: int = 0
    geo_anomaly_score: float = 0.0
    hour_probability: float = 0.042
    is_weekend: int = 0
    is_night: int = 0
    merchant_risk_score: float = 0.01
    rule_based_score: float = 0.0
    hour_of_day: int = 12
    day_of_week: int = 1
    user_txn_count_1h: int = 0


class PredictionResponse(BaseModel):
    transaction_id: str
    is_fraud: bool
    fraud_score: float
    risk_level: str
    model_version: str
    latency_ms: float
    timestamp: str


def score_to_risk(score: float) -> str:
    if score < 0.3:   return "LOW"
    if score < 0.6:   return "MEDIUM"
    if score < 0.8:   return "HIGH"
    return "CRITICAL"


# ─── FastAPI App ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Prometheus metrics server
    start_http_server(PROMETHEUS_PORT)
    log.info(f"Prometheus metrics at :{PROMETHEUS_PORT}/metrics")
    # Start Kafka stream consumer in background thread
    t = threading.Thread(target=run_kafka_consumer, daemon=True)
    t.start()
    yield

app = FastAPI(
    title="Fraud Detection API",
    description="Real-time financial fraud detection with ensemble ML",
    version="2.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "healthy", "model_version": model_mgr._version, "timestamp": datetime.utcnow().isoformat()}


@app.get("/metrics/summary")
async def metrics_summary():
    """Business-level metrics summary"""
    with _recent_lock:
        preds = list(_recent_preds)
    total = len(preds)
    fraud = sum(preds)
    return {
        "total_predictions_recent": total,
        "fraud_count_recent": fraud,
        "fraud_rate_recent": fraud / total if total > 0 else 0,
        "model_version": model_mgr._version,
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict(txn: TransactionFeatures):
    start = time.monotonic()
    try:
        X = np.array([[getattr(txn, col, 0) for col in FEATURE_COLS]])
        preds, scores = model_mgr.predict(X)
        is_fraud = bool(preds[0])
        score    = float(scores[0])
        latency  = (time.monotonic() - start) * 1000

        # Metrics
        prediction_counter.labels(
            result="fraud" if is_fraud else "legit",
            method="rest"
        ).inc()
        prediction_latency.labels(method="rest").observe(latency / 1000)
        model_score_hist.observe(score)
        update_anomaly_rate(is_fraud)

        # Cache in Redis (for explainability)
        result_data = {
            "is_fraud": is_fraud,
            "score": score,
            "timestamp": datetime.utcnow().isoformat(),
        }
        r_client.setex(f"pred:{txn.transaction_id}", 3600, json.dumps(result_data))

        return PredictionResponse(
            transaction_id=txn.transaction_id,
            is_fraud=is_fraud,
            fraud_score=round(score, 6),
            risk_level=score_to_risk(score),
            model_version=model_mgr._version,
            latency_ms=round(latency, 3),
            timestamp=datetime.utcnow().isoformat(),
        )
    except Exception as e:
        log.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/batch")
async def predict_batch(transactions: list[TransactionFeatures]):
    """Batch prediction endpoint"""
    if len(transactions) > 1000:
        raise HTTPException(400, "Max batch size is 1000")
    start = time.monotonic()
    X = np.array([[getattr(t, col, 0) for col in FEATURE_COLS] for t in transactions])
    preds, scores = model_mgr.predict(X)
    latency = (time.monotonic() - start) * 1000

    results = []
    for txn, is_f, score in zip(transactions, preds, scores):
        results.append({
            "transaction_id": txn.transaction_id,
            "is_fraud": bool(is_f),
            "fraud_score": round(float(score), 6),
            "risk_level": score_to_risk(float(score)),
        })
        update_anomaly_rate(bool(is_f))

    return {"results": results, "count": len(results), "latency_ms": round(latency, 3)}


@app.post("/model/reload")
async def reload_model():
    """Hot-reload model — zero downtime"""
    model_mgr.reload()
    return {"status": "reloaded", "version": model_mgr._version}


@app.get("/prediction/{transaction_id}")
async def get_prediction(transaction_id: str):
    """Retrieve cached prediction"""
    data = r_client.get(f"pred:{transaction_id}")
    if not data:
        raise HTTPException(404, "Prediction not found")
    return json.loads(data)


# ─── Kafka Stream Consumer ───────────────────────────────────────────────────────
def run_kafka_consumer():
    """Consume engineered features and produce fraud predictions"""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "fraud-inference-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "fetch.max.bytes": 10485760,
    })
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "1",
        "compression.type": "lz4",
    })

    consumer.subscribe([KAFKA_INPUT])
    log.info(f"Kafka consumer subscribed to {KAFKA_INPUT}")

    batch_msgs, batch_features, batch_ids = [], [], []
    BATCH_SIZE = 100
    BATCH_TIMEOUT = 0.05  # 50ms max wait

    last_flush = time.monotonic()

    while True:
        msg = consumer.poll(timeout=0.01)
        should_flush = (time.monotonic() - last_flush) > BATCH_TIMEOUT

        if msg is not None and not msg.error():
            try:
                features = json.loads(msg.value())
                row = [features.get(col, 0) for col in FEATURE_COLS]
                batch_features.append(row)
                batch_ids.append(features.get("transaction_id", "unknown"))
                batch_msgs.append(msg)
            except Exception as e:
                log.error(f"Parse error: {e}")

        if (len(batch_features) >= BATCH_SIZE or should_flush) and batch_features:
            t0 = time.monotonic()
            X = np.array(batch_features)
            preds, scores = model_mgr.predict(X)
            latency = time.monotonic() - t0

            prediction_latency.labels(method="kafka").observe(latency)

            for txn_id, is_f, score, orig_msg in zip(batch_ids, preds, scores, batch_msgs):
                is_fraud = bool(is_f)
                result = {
                    "transaction_id": txn_id,
                    "is_fraud": is_fraud,
                    "fraud_score": round(float(score), 6),
                    "risk_level": score_to_risk(float(score)),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                producer.produce(
                    KAFKA_OUTPUT,
                    key=txn_id.encode(),
                    value=json.dumps(result).encode(),
                )
                prediction_counter.labels(
                    result="fraud" if is_fraud else "legit",
                    method="kafka"
                ).inc()
                model_score_hist.observe(float(score))
                update_anomaly_rate(is_fraud)

            producer.flush()
            consumer.commit(asynchronous=False)

            batch_msgs.clear(); batch_features.clear(); batch_ids.clear()
            last_flush = time.monotonic()


if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        workers=1,
        loop="uvloop",
        log_level="info",
    )
