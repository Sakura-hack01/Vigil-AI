#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'; BOLD='\033[1m'

banner() {
  echo -e "\n${BOLD}${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}${GREEN}║  🔐 Vigil AI                                   v2.1           ║${NC}"
  echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}\n"
}

log() { echo -e "${GREEN}[$(date +%H:%M:%S)] ✓ $1${NC}"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)] ⚠ $1${NC}"; }
err() { echo -e "${RED}[$(date +%H:%M:%S)] ✗ $1${NC}"; }
section() { echo -e "\n${BOLD}━━━ $1 ━━━${NC}"; }

banner

section "Prerequisites Check"
for cmd in docker docker-compose curl python3; do
  if command -v $cmd &>/dev/null; then
    log "$cmd found"
  else
    err "$cmd not found — please install it"
    exit 1
  fi
done

section "Starting Infrastructure"
docker-compose up -d zookeeper kafka redis postgres
log "Waiting for Kafka + Postgres to be healthy..."
sleep 20

section "Training Model"
log "Starting model trainer (this takes ~60s)..."
docker-compose up -d model-trainer
echo -n "  Waiting for model"
for i in {1..30}; do
  if [ -f "./ml-service/models/fraud_model.pkl" ]; then
    echo ""
    log "Model trained and saved!"
    break
  fi
  echo -n "."
  sleep 5
done

section "Starting Full Pipeline"
docker-compose up -d

log "Waiting for services to stabilize..."
sleep 30

section "Health Checks"
check_service() {
  local name=$1 url=$2
  if curl -sf "$url" >/dev/null 2>&1; then
    log "$name is healthy"
  else
    warn "$name not yet ready at $url"
  fi
}

check_service "Fraud Detector API"  "http://localhost:8000/health"
check_service "Kafka UI"            "http://localhost:8090"
check_service "Flink Dashboard"     "http://localhost:8081"
check_service "Grafana"             "http://localhost:3000"
check_service "Prometheus"          "http://localhost:9090"

section "Running Demo Predictions"
echo ""
echo "Single transaction prediction:"
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-001",
    "user_id": "user_00001",
    "amount": 4500.00,
    "amount_ratio_10txn": 12.5,
    "amount_zscore_10txn": 8.2,
    "velocity_1h": 15,
    "velocity_24h": 20,
    "new_device_flag": 1,
    "cross_border_flag": 1,
    "geo_anomaly_score": 0.85,
    "rule_based_score": 0.75,
    "merchant_risk_score": 0.08
  }' | python3 -m json.tool

echo ""
echo "Metrics summary:"
curl -s http://localhost:8000/metrics/summary | python3 -m json.tool

section "Pipeline URLs"
echo ""
echo -e "  🔍 ${BOLD}Fraud API Docs${NC}:     http://localhost:8000/docs"
echo -e "  📊 ${BOLD}Grafana Dashboard${NC}:  http://localhost:3000  (admin/admin)"
echo -e "  📡 ${BOLD}Prometheus${NC}:         http://localhost:9090"
echo -e "  ⚡ ${BOLD}Kafka UI${NC}:           http://localhost:8090"
echo -e "  🌊 ${BOLD}Flink Dashboard${NC}:    http://localhost:8081"
echo -e "  🚨 ${BOLD}Alertmanager${NC}:       http://localhost:9093"
echo ""
log "Pipeline is LIVE! Producing ${TPS:-200} transactions/second."
echo ""
