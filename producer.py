"""
Advanced Transaction Producer
- Simulates realistic financial transactions
- Injects synthetic fraud patterns (card takeover, velocity attacks, geographic anomalies)
- Simulates concept drift (spending pattern shifts over time)
- Produces to Kafka with Avro schema
"""

import os
import json
import time
import random
import uuid
import math
import logging
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("producer")

# ─── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_TXN          = os.getenv("TOPIC_TRANSACTIONS", "raw-transactions")
TOPIC_LABELS       = os.getenv("TOPIC_LABELS", "ground-truth-labels")
TPS                = int(os.getenv("TPS", "200"))
FRAUD_RATE         = float(os.getenv("FRAUD_RATE", "0.02"))
CONCEPT_DRIFT      = os.getenv("CONCEPT_DRIFT", "true").lower() == "true"

# ─── Merchant categories & weights ─────────────────────────────────────────────
CATEGORIES = {
    "grocery":        {"weight": 0.25, "avg": 65,   "std": 30},
    "restaurant":     {"weight": 0.20, "avg": 45,   "std": 25},
    "gas_station":    {"weight": 0.12, "avg": 55,   "std": 20},
    "online_retail":  {"weight": 0.18, "avg": 120,  "std": 80},
    "pharmacy":       {"weight": 0.08, "avg": 35,   "std": 20},
    "entertainment":  {"weight": 0.07, "avg": 80,   "std": 50},
    "travel":         {"weight": 0.05, "avg": 400,  "std": 300},
    "atm":            {"weight": 0.05, "avg": 200,  "std": 100},
}

COUNTRIES = ["US", "US", "US", "US", "GB", "CA", "FR", "DE", "RU", "NG", "CN", "BR"]
FRAUD_COUNTRIES = ["RU", "NG", "CN", "BR", "UA", "VN"]

# ─── User profiles ─────────────────────────────────────────────────────────────
N_USERS = 5000
N_MERCHANTS = 500


@dataclass
class Transaction:
    transaction_id: str
    user_id: str
    merchant_id: str
    merchant_category: str
    amount: float
    currency: str
    country: str
    city: str
    card_present: bool
    device_fingerprint: str
    ip_address: str
    timestamp: str
    hour_of_day: int
    day_of_week: int
    # enriched fields
    user_avg_txn_30d: float
    user_txn_count_1h: int
    merchant_fraud_rate_7d: float


@dataclass
class Label:
    transaction_id: str
    is_fraud: bool
    fraud_type: Optional[str]
    timestamp: str


class UserProfile:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.home_country = random.choice(["US"] * 8 + ["GB", "CA", "FR", "DE"])
        self.income_tier = random.choice(["low", "mid", "high"])
        self.avg_txn = {"low": 40, "mid": 80, "high": 200}[self.income_tier]
        self.preferred_categories = random.sample(list(CATEGORIES.keys()), k=4)
        self.usual_hours = list(range(8, 23))  # Active hours
        self.device_fingerprint = str(uuid.uuid4())[:12]
        self.base_ip = f"{random.randint(100,199)}.{random.randint(1,254)}"

    def get_ip(self, is_fraud=False):
        if is_fraud:
            return f"{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
        return f"{self.base_ip}.{random.randint(1,254)}.{random.randint(1,254)}"


class FraudPatternEngine:
    """Generates various fraud attack patterns"""

    @staticmethod
    def card_takeover(user: UserProfile) -> dict:
        """Account taken over — transactions from foreign location, unusual amounts"""
        return {
            "amount": random.uniform(500, 5000),
            "country": random.choice(FRAUD_COUNTRIES),
            "card_present": False,
            "device_fingerprint": str(uuid.uuid4())[:12],  # new device
            "merchant_category": "online_retail",
            "fraud_type": "card_takeover",
        }

    @staticmethod
    def velocity_attack(user: UserProfile) -> dict:
        """Many small transactions in rapid succession"""
        return {
            "amount": random.uniform(1, 15),
            "country": user.home_country,
            "card_present": False,
            "device_fingerprint": str(uuid.uuid4())[:12],
            "merchant_category": "online_retail",
            "fraud_type": "velocity_attack",
        }

    @staticmethod
    def atm_cashout(user: UserProfile) -> dict:
        """Large ATM withdrawals at unusual hours"""
        return {
            "amount": random.choice([200, 400, 500, 1000]),
            "country": random.choice(FRAUD_COUNTRIES),
            "card_present": True,
            "device_fingerprint": user.device_fingerprint,
            "merchant_category": "atm",
            "fraud_type": "atm_cashout",
        }

    @staticmethod
    def synthetic_identity(user: UserProfile) -> dict:
        """Synthetic identity — gradual buildup then large purchase"""
        return {
            "amount": random.uniform(2000, 8000),
            "country": user.home_country,
            "card_present": False,
            "device_fingerprint": user.device_fingerprint,
            "merchant_category": "travel",
            "fraud_type": "synthetic_identity",
        }


class ConceptDriftSimulator:
    """Simulates distribution shifts in transaction patterns"""

    def __init__(self):
        self.start_time = time.time()
        self.drift_phase = 0

    def get_drift_multiplier(self) -> float:
        """Returns amount multiplier — drifts over time to simulate behavior change"""
        if not CONCEPT_DRIFT:
            return 1.0
        elapsed = (time.time() - self.start_time) / 3600  # hours elapsed
        # Slow sinusoidal drift + gradual increase (simulates holiday spending)
        drift = 1.0 + 0.3 * math.sin(elapsed * math.pi / 12) + 0.05 * elapsed
        return max(0.5, min(drift, 3.0))

    def get_fraud_rate(self) -> float:
        elapsed = (time.time() - self.start_time) / 3600
        # Fraud rate spikes during simulated "attacks"
        spike = 0.05 if int(elapsed) % 6 == 5 else 0.0
        return FRAUD_RATE + spike


class TransactionProducer:
    def __init__(self):
        self.users = {f"user_{i:05d}": UserProfile(f"user_{i:05d}") for i in range(N_USERS)}
        self.merchants = [f"merch_{i:04d}" for i in range(N_MERCHANTS)]
        self.merchant_fraud_rates = {m: random.uniform(0.001, 0.05) for m in self.merchants}
        self.user_txn_history = {}  # user_id -> list of recent amounts
        self.user_hour_counts = {}  # user_id -> {hour: count}
        self.drift_sim = ConceptDriftSimulator()
        self.fraud_engine = FraudPatternEngine()
        self.stats = {"total": 0, "fraud": 0, "errors": 0}

        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "compression.type": "lz4",
            "batch.size": 65536,
            "linger.ms": 5,
            "retries": 5,
        }
        self.producer = Producer(conf)
        self._ensure_topics()

    def _ensure_topics(self):
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        topics = [
            NewTopic(TOPIC_TXN,    num_partitions=12, replication_factor=1),
            NewTopic(TOPIC_LABELS, num_partitions=12, replication_factor=1),
        ]
        fs = admin.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                log.info(f"Topic '{topic}' created")
            except Exception as e:
                log.debug(f"Topic '{topic}': {e}")

    def _get_user_avg(self, user_id: str) -> float:
        history = self.user_txn_history.get(user_id, [])
        return np.mean(history[-30:]) if history else self.users[user_id].avg_txn

    def _get_user_hour_count(self, user_id: str, hour: int) -> int:
        counts = self.user_hour_counts.get(user_id, {})
        return counts.get(hour, 0)

    def _update_history(self, user_id: str, amount: float, hour: int):
        if user_id not in self.user_txn_history:
            self.user_txn_history[user_id] = []
        self.user_txn_history[user_id].append(amount)
        if len(self.user_txn_history[user_id]) > 100:
            self.user_txn_history[user_id].pop(0)

        if user_id not in self.user_hour_counts:
            self.user_hour_counts[user_id] = {}
        self.user_hour_counts[user_id][hour] = self.user_hour_counts[user_id].get(hour, 0) + 1

    def _generate_normal_txn(self, user: UserProfile, drift_mult: float) -> dict:
        cat_name = random.choice(user.preferred_categories)
        cat = CATEGORIES[cat_name]
        raw_amount = max(1.0, np.random.normal(cat["avg"], cat["std"]))
        amount = round(raw_amount * drift_mult, 2)
        hour = random.choice(user.usual_hours)
        return {
            "amount": amount,
            "merchant_category": cat_name,
            "country": user.home_country,
            "card_present": random.random() > 0.4,
            "device_fingerprint": user.device_fingerprint,
            "hour": hour,
        }

    def _make_transaction(self) -> tuple[Transaction, Label]:
        user_id = random.choice(list(self.users.keys()))
        user = self.users[user_id]
        merchant_id = random.choice(self.merchants)
        now = datetime.now(timezone.utc)
        drift_mult = self.drift_sim.get_drift_multiplier()
        fraud_rate = self.drift_sim.get_fraud_rate()

        is_fraud = random.random() < fraud_rate
        fraud_type = None

        if is_fraud:
            fraud_pattern = random.choice([
                self.fraud_engine.card_takeover,
                self.fraud_engine.velocity_attack,
                self.fraud_engine.atm_cashout,
                self.fraud_engine.synthetic_identity,
            ])
            overrides = fraud_pattern(user)
            fraud_type = overrides.pop("fraud_type")
            base = self._generate_normal_txn(user, drift_mult)
            base.update(overrides)
            txn_data = base
        else:
            txn_data = self._generate_normal_txn(user, drift_mult)

        hour = txn_data.get("hour", now.hour)
        user_avg = self._get_user_avg(user_id)
        hour_count = self._get_user_hour_count(user_id, now.hour)

        txn = Transaction(
            transaction_id=str(uuid.uuid4()),
            user_id=user_id,
            merchant_id=merchant_id,
            merchant_category=txn_data["merchant_category"],
            amount=txn_data["amount"],
            currency="USD",
            country=txn_data["country"],
            city=f"City_{random.randint(1, 100)}",
            card_present=txn_data["card_present"],
            device_fingerprint=txn_data["device_fingerprint"],
            ip_address=user.get_ip(is_fraud),
            timestamp=now.isoformat(),
            hour_of_day=hour,
            day_of_week=now.weekday(),
            user_avg_txn_30d=round(user_avg, 2),
            user_txn_count_1h=hour_count,
            merchant_fraud_rate_7d=round(self.merchant_fraud_rates[merchant_id], 5),
        )
        label = Label(
            transaction_id=txn.transaction_id,
            is_fraud=is_fraud,
            fraud_type=fraud_type,
            timestamp=now.isoformat(),
        )
        self._update_history(user_id, txn.amount, now.hour)
        return txn, label

    def _delivery_report(self, err, msg):
        if err is not None:
            self.stats["errors"] += 1
            log.error(f"Delivery failed: {err}")

    def _stats_reporter(self):
        while True:
            time.sleep(10)
            total = self.stats["total"]
            fraud = self.stats["fraud"]
            rate = fraud / total * 100 if total > 0 else 0
            drift = self.drift_sim.get_drift_multiplier()
            log.info(f"Stats | Total={total} | Fraud={fraud} ({rate:.2f}%) | Drift={drift:.2f}x | Errors={self.stats['errors']}")

    def run(self):
        log.info(f"Starting producer: {TPS} TPS → {KAFKA_BOOTSTRAP}")
        thread = threading.Thread(target=self._stats_reporter, daemon=True)
        thread.start()

        interval = 1.0 / TPS
        while True:
            loop_start = time.monotonic()
            try:
                txn, label = self._make_transaction()
                txn_payload = json.dumps(asdict(txn)).encode()
                label_payload = json.dumps(asdict(label)).encode()

                self.producer.produce(
                    TOPIC_TXN,
                    key=txn.user_id.encode(),
                    value=txn_payload,
                    callback=self._delivery_report,
                )
                self.producer.produce(
                    TOPIC_LABELS,
                    key=txn.transaction_id.encode(),
                    value=label_payload,
                )
                self.producer.poll(0)
                self.stats["total"] += 1
                if label.is_fraud:
                    self.stats["fraud"] += 1
            except Exception as e:
                log.error(f"Error generating transaction: {e}")
                self.stats["errors"] += 1

            elapsed = time.monotonic() - loop_start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)


if __name__ == "__main__":
    # Wait for Kafka to be ready
    log.info("Waiting for Kafka...")
    time.sleep(15)
    producer = TransactionProducer()
    producer.run()
