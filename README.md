# 🔐 Vigil AI

> Real-time anomaly detection system with Kafka, Flink-style feature engineering, ensemble ML, gRPC inference, and full MLOps observability.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                    Vigil AI                       │
└─────────────────────────────────────────────────────────────────────────────┘

 LAYER 1: DATA INGESTION                    LAYER 2: FEATURE ENGINEERING
 ─────────────────────────                  ─────────────────────────────
 ┌──────────────────────┐                   ┌─────────────────────────────┐
 │  Transaction         │  raw-transactions  │  Flink-Style Processor      │
 │  Producer            │──────────────────▶│  - Amount ratio / Z-score   │
 │                      │                   │  - Velocity 1h / 24h        │
 │  • 200 TPS           │                   │  - New device flag          │
 │  • 4 fraud patterns  │                   │  - Geo anomaly score        │
 │  • Concept drift sim │                   │  - CUSUM drift detection    │
 │  • Kafka (LZ4)       │                   │  - Redis feature cache      │
 └──────────────────────┘                   └──────────────┬──────────────┘
                                                           │ engineered-features
                                                           ▼
 LAYER 4: MLOPS / OBSERVABILITY            LAYER 3: INFERENCE
 ──────────────────────────────             ─────────────────────────────────
 ┌──────────────────────────┐               ┌──────────────────────────────┐
 │  Drift Detector           │               │  Ensemble Model Server        │
 │  • KS Test per feature   │               │                               │
 │  • PSI score tracking    │◀──────────────│  • Isolation Forest          │
 │  • Wasserstein distance  │ fraud-preds   │  • Autoencoder (PCA recon.)  │
 │  • CUSUM gradual drift   │               │  • XGBoost classifier        │
 │  • Auto-retrain trigger  │               │  • Threshold calibration     │
 └───────────┬──────────────┘               │  • FastAPI REST + gRPC       │
             │                              │  • Kafka stream consumer     │
             ▼                              │  • Hot model reload          │
 ┌──────────────────────────┐               └──────────────────────────────┘
 │  Observability Stack      │
 │  • Prometheus metrics    │
 │  • Grafana dashboards    │
 │  • Alertmanager rules    │
 │  • Slack webhooks        │
 └──────────────────────────┘
```

---

## Advanced Features (Beyond the Spec)

### 1. Four Fraud Pattern Types
The producer generates 4 distinct attack patterns automatically:
- **Card Takeover** — new device + foreign country + large amount
- **Velocity Attack** — many small transactions in rapid succession  
- **ATM Cashout** — large ATM withdrawals from fraud-prone countries
- **Synthetic Identity** — slow buildup then large purchase

### 2. Concept Drift Simulation
The producer uses a sinusoidal + linear drift function to simulate:
- Seasonal spending changes (holidays, recessions)
- Periodic fraud spikes (every 6 simulated hours)

### 3. Three-Method Drift Detection
Unlike single-test approaches, this uses three statistical tests simultaneously:
- **Kolmogorov-Smirnov** — overall distribution shape
- **Population Stability Index (PSI)** — bucket-level drift
- **Wasserstein Distance** — Earth mover's distance between distributions
- **CUSUM** — detects *gradual* drift (invisible to point-in-time tests)

### 4. Ensemble Model (3 Components)
| Component | Weight | Role |
|-----------|--------|------|
| Isolation Forest | 40% | Unsupervised outlier detection |
| Autoencoder (PCA) | 20% | Reconstruction error anomaly |
| XGBoost | 40% | Supervised fraud classifier |

Threshold calibrated to achieve **≥90% recall** on validation set.

### 5. gRPC + REST Dual Interface
- **REST** — `/predict` for human-readable single/batch predictions
- **gRPC** (port 50051) — sub-10ms batch inference for HFT integrations
- **Kafka Stream** — auto-consumes engineered features, produces labels

### 6. Hot Model Reload (Zero Downtime)
```bash
curl -X POST http://localhost:8000/model/reload
```
Swaps model in memory with an RLock — zero dropped requests.

---

## Quick Start

```bash
# One command
bash scripts/start.sh

# Or manual
docker-compose up -d
```

Wait ~90 seconds for model training. Then:

| Service | URL |
|---------|-----|
| Fraud API (Swagger) | http://localhost:8000/docs |
| Grafana Dashboard | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| Kafka UI | http://localhost:8090 |
| Flink Dashboard | http://localhost:8081 |
| Alertmanager | http://localhost:9093 |

---

## API Usage

### Single Prediction
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn-abc123",
    "user_id": "user_00001",
    "amount": 4500.00,
    "amount_ratio_10txn": 12.5,
    "velocity_1h": 15,
    "new_device_flag": 1,
    "cross_border_flag": 1,
    "geo_anomaly_score": 0.85
  }'
```

### Response
```json
{
  "transaction_id": "txn-abc123",
  "is_fraud": true,
  "fraud_score": 0.923,
  "risk_level": "CRITICAL",
  "model_version": "2.1.0",
  "latency_ms": 3.2,
  "timestamp": "2025-01-01T00:00:00Z"
}
```

### Batch Prediction (up to 1000)
```bash
curl -X POST http://localhost:8000/predict/batch \
  -H "Content-Type: application/json" \
  -d '{"transactions": [...]}'
```

---

## Kubernetes Deployment

```bash
# Build images
docker build -t fraud-pipeline/producer:latest ./kafka
docker build -t fraud-pipeline/feature-job:latest ./flink  
docker build -t fraud-pipeline/fraud-detector:latest ./ml-service --file ml-service/Dockerfile.server
docker build -t fraud-pipeline/drift-detector:latest ./monitoring --file monitoring/Dockerfile.drift

# Deploy
kubectl apply -f k8s/deployment.yaml

# HPA scales 2→20 pods automatically under load
kubectl get hpa -n fraud-detection
```

---

## Grafana Alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| HighFraudRate | Rate > 10% for 2m | Critical |
| LowFraudRate | Rate < 0.1% for 5m | Warning |
| HighLatency REST | P99 > 50ms | Warning |
| KafkaLatencyHigh | P99 > 10ms | Warning |
| PredictionRateDrop | < 1 TPS for 3m | Critical |
| DataDrift | Rate +50% vs 30min ago | Warning |

---

## Project Structure

```
Vigil-AI/
├── kafka/
│   ├── producer.py          # Stream producer (4 fraud patterns, drift sim)
│   └── Dockerfile.producer
├── flink/
│   ├── feature_job.py       # 20+ engineered features, Redis feature store
│   ├── Dockerfile.flink
│   └── Dockerfile.job
├── ml-service/
│   ├── trainer.py           # Isolation Forest + Autoencoder + XGBoost ensemble
│   ├── server.py            # FastAPI + gRPC + Kafka consumer + Prometheus
│   ├── Dockerfile.trainer
│   └── Dockerfile.server
├── monitoring/
│   ├── drift_detector.py    # KS + PSI + Wasserstein + CUSUM + auto-retrain
│   ├── prometheus.yml       # Scrape configs
│   ├── alert_rules.yml      # 6 production alert rules
│   ├── alertmanager.yml     # Slack routing
│   └── grafana/             # Pre-provisioned dashboards
├── k8s/
│   └── deployment.yaml      # Full K8s with HPA (2→20 pods)
├── scripts/
│   ├── init_db.sql          # Postgres schema
│   └── start.sh             # One-command startup
└── docker-compose.yml       # Full local orchestration
```
