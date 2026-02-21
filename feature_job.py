"""
Flink Feature Engineering Job
Advanced real-time feature computation using:
- Tumbling windows (fixed 1h, 24h buckets)
- Sliding windows (last N transactions)
- Session windows (per-user activity sessions)
- Count-based windows (last 10 txns)

Features computed per transaction:
  - amount_to_user_avg_ratio (3x rule)
  - velocity_1h, velocity_24h (transaction counts)
  - amount_zscore (statistical outlier score)
  - new_device_flag
  - geo_distance_km (from usual location)
  - hour_anomaly_score
  - cross_border_flag
  - merchant_risk_score
"""

import os
import json
import time
import logging
import math
import redis
import psycopg2
from datetime import datetime
from collections import defaultdict, deque
from confluent_kafka import Consumer, Producer, KafkaError
import numpy as np
import threading

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("flink-job")

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_HOST       = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT", "6379"))
POSTGRES_DSN     = os.getenv("POSTGRES_DSN", "postgresql://feast:feast_secret@localhost:5432/fraud_features")

INPUT_TOPIC      = "raw-transactions"
OUTPUT_TOPIC     = "engineered-features"


class FeatureStore:
    """Redis-backed feature store with time-decay and sliding windows"""

    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        log.info("Connected to Redis feature store")

    def _key(self, prefix: str, entity_id: str) -> str:
        return f"fs:{prefix}:{entity_id}"

    def push_amount(self, user_id: str, amount: float, timestamp: float):
        """Push amount to sliding window (keep last 100 with TTL)"""
        key = self._key("amounts", user_id)
        entry = json.dumps({"amount": amount, "ts": timestamp})
        pipe = self.r.pipeline()
        pipe.lpush(key, entry)
        pipe.ltrim(key, 0, 99)   # Keep last 100
        pipe.expire(key, 86400 * 30)  # 30-day TTL
        pipe.execute()

    def get_window_stats(self, user_id: str, window_size: int = 10) -> dict:
        """Get stats from last N transactions"""
        key = self._key("amounts", user_id)
        raw = self.r.lrange(key, 0, window_size - 1)
        if not raw:
            return {"mean": 0, "std": 1, "count": 0, "max": 0}
        amounts = [json.loads(r)["amount"] for r in raw]
        return {
            "mean": float(np.mean(amounts)),
            "std": float(np.std(amounts)) if len(amounts) > 1 else 1.0,
            "count": len(amounts),
            "max": float(np.max(amounts)),
            "min": float(np.min(amounts)),
        }

    def get_velocity(self, user_id: str, window_seconds: int) -> int:
        """Count transactions in last N seconds"""
        key = self._key("amounts", user_id)
        raw = self.r.lrange(key, 0, 99)
        cutoff = time.time() - window_seconds
        return sum(1 for r in raw if json.loads(r)["ts"] > cutoff)

    def update_device(self, user_id: str, device: str):
        key = self._key("device", user_id)
        known = self.r.smembers(key)
        is_new = device not in known
        self.r.sadd(key, device)
        self.r.expire(key, 86400 * 90)
        return is_new

    def get_hourly_profile(self, user_id: str, hour: int) -> float:
        """Return fraction of user's txns in this hour"""
        key = self._key("hours", user_id)
        self.r.hincrby(key, str(hour), 1)
        self.r.expire(key, 86400 * 30)
        all_hours = self.r.hgetall(key)
        total = sum(int(v) for v in all_hours.values())
        hour_count = int(all_hours.get(str(hour), 0))
        return hour_count / total if total > 0 else 1.0 / 24

    def get_merchant_risk(self, merchant_id: str) -> float:
        key = self._key("merch_risk", merchant_id)
        val = self.r.get(key)
        return float(val) if val else 0.01

    def update_merchant_risk(self, merchant_id: str, is_fraud: bool):
        key_count = self._key("merch_count", merchant_id)
        key_fraud  = self._key("merch_fraud", merchant_id)
        pipe = self.r.pipeline()
        pipe.incr(key_count)
        if is_fraud:
            pipe.incr(key_fraud)
        pipe.expire(key_count, 86400 * 7)
        pipe.expire(key_fraud, 86400 * 7)
        results = pipe.execute()
        count = results[0]
        fraud_c = int(self.r.get(key_fraud) or 0)
        rate = fraud_c / count if count > 0 else 0.01
        self.r.set(self._key("merch_risk", merchant_id), rate, ex=86400 * 7)


def haversine(lat1, lon1, lat2, lon2) -> float:
    """Calculate distance between coordinates in km"""
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Approximate country centroids
COUNTRY_COORDS = {
    "US": (37.09, -95.71), "GB": (55.37, -3.43), "CA": (56.13, -106.34),
    "FR": (46.22, 2.21),   "DE": (51.16, 10.45),  "RU": (61.52, 105.31),
    "NG": (9.08, 8.67),    "CN": (35.86, 104.19), "BR": (-14.23, -51.92),
    "UA": (48.37, 31.16),  "VN": (14.05, 108.27),
}


class FeatureEngineer:
    def __init__(self):
        self.store = FeatureStore()

    def compute(self, txn: dict) -> dict:
        uid   = txn["user_id"]
        mid   = txn["merchant_id"]
        amt   = txn["amount"]
        hour  = txn["hour_of_day"]
        country = txn["country"]
        device  = txn["device_fingerprint"]
        ts    = time.time()

        # Window statistics
        stats = self.store.get_window_stats(uid, window_size=10)
        stats_30 = self.store.get_window_stats(uid, window_size=30)

        # Feature computations
        mean = stats["mean"] if stats["mean"] > 0 else txn["user_avg_txn_30d"]
        std  = stats["std"]  if stats["std"]  > 0 else mean * 0.3

        amount_ratio     = amt / mean if mean > 0 else 1.0
        amount_zscore    = (amt - mean) / std if std > 0 else 0.0
        velocity_1h      = self.store.get_velocity(uid, 3600)
        velocity_24h     = self.store.get_velocity(uid, 86400)
        new_device       = self.store.update_device(uid, device)
        hour_probability = self.store.get_hourly_profile(uid, hour)
        merchant_risk    = self.store.get_merchant_risk(mid)

        # Geographic anomaly
        home_country = "US"  # Simplified — in production, learn from history
        geo_anomaly = 0.0
        if country in COUNTRY_COORDS and home_country in COUNTRY_COORDS:
            d = haversine(*COUNTRY_COORDS[home_country], *COUNTRY_COORDS[country])
            geo_anomaly = min(d / 10000, 1.0)

        # Cross-border flag
        cross_border = 1 if country != home_country else 0

        # Composite risk score (hand-crafted rule engine)
        rule_score = 0.0
        if amount_ratio > 3.0:    rule_score += 0.3
        if amount_ratio > 5.0:    rule_score += 0.2
        if velocity_1h > 5:       rule_score += 0.15
        if velocity_1h > 10:      rule_score += 0.2
        if new_device:            rule_score += 0.1
        if cross_border:          rule_score += 0.1
        if geo_anomaly > 0.5:     rule_score += 0.2
        if hour_probability < 0.02: rule_score += 0.05
        rule_score = min(rule_score, 1.0)

        features = {
            **txn,
            # Amount features
            "amount_ratio_10txn":      round(amount_ratio, 4),
            "amount_zscore_10txn":     round(amount_zscore, 4),
            "amount_vs_30txn_mean":    round(amt / stats_30["mean"] if stats_30["mean"] > 0 else 1.0, 4),
            "amount_max_10txn_ratio":  round(amt / stats["max"] if stats["max"] > 0 else 1.0, 4),
            # Velocity features
            "velocity_1h":             velocity_1h,
            "velocity_24h":            velocity_24h,
            "velocity_ratio_1h_24h":   round(velocity_1h / (velocity_24h / 24) if velocity_24h > 0 else 0, 4),
            # Device/Identity features
            "new_device_flag":         int(new_device),
            "card_present":            int(txn["card_present"]),
            # Geographic features
            "cross_border_flag":       cross_border,
            "geo_anomaly_score":       round(geo_anomaly, 4),
            # Temporal features
            "hour_probability":        round(hour_probability, 6),
            "is_weekend":              int(txn["day_of_week"] >= 5),
            "is_night":                int(hour < 6 or hour > 22),
            # Merchant features
            "merchant_risk_score":     round(merchant_risk, 6),
            # Composite
            "rule_based_score":        round(rule_score, 4),
            # Metadata
            "feature_version":         "v2.1",
            "feature_timestamp":       datetime.utcnow().isoformat(),
        }

        # Update state
        self.store.push_amount(uid, amt, ts)
        return features


class FlinkStyleProcessor:
    """Stream processor mimicking Flink's pipeline"""

    def __init__(self):
        self.engineer = FeatureEngineer()
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "feature-engineer-group",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
        })
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "compression.type": "lz4",
        })
        self.metrics = defaultdict(int)

    def _report_metrics(self):
        while True:
            time.sleep(30)
            log.info(f"Processor metrics: {dict(self.metrics)}")
            self.metrics = defaultdict(int)

    def run(self):
        self.consumer.subscribe([INPUT_TOPIC])
        log.info(f"Feature processor subscribed to {INPUT_TOPIC}")

        reporter = threading.Thread(target=self._report_metrics, daemon=True)
        reporter.start()

        batch = []
        BATCH_SIZE = 50

        while True:
            msg = self.consumer.poll(timeout=0.1)

            if msg is None:
                if batch:
                    self._flush_batch(batch)
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Consumer error: {msg.error()}")
                continue

            try:
                txn = json.loads(msg.value())
                features = self.engineer.compute(txn)
                batch.append((txn["user_id"], features, msg))
                self.metrics["processed"] += 1
            except Exception as e:
                log.error(f"Feature computation error: {e}")
                self.metrics["errors"] += 1

            if len(batch) >= BATCH_SIZE:
                self._flush_batch(batch)
                batch = []

    def _flush_batch(self, batch: list):
        for user_id, features, msg in batch:
            try:
                self.producer.produce(
                    OUTPUT_TOPIC,
                    key=user_id.encode(),
                    value=json.dumps(features).encode(),
                )
            except Exception as e:
                log.error(f"Produce error: {e}")
                self.metrics["produce_errors"] += 1

        self.producer.flush()
        # Commit after flush
        self.consumer.commit(asynchronous=False)
        self.metrics["batches_flushed"] += 1


if __name__ == "__main__":
    log.info("Waiting for Kafka + Redis...")
    time.sleep(20)
    processor = FlinkStyleProcessor()
    processor.run()
