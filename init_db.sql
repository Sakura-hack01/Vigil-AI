-- Fraud Detection Feature Store Schema
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Materialized feature snapshots for Feast integration
CREATE TABLE IF NOT EXISTS entity_df (
    entity_id       TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id, event_timestamp)
);

-- Historical feature storage (for training + drift reference)
CREATE TABLE IF NOT EXISTS transaction_features (
    transaction_id          TEXT PRIMARY KEY,
    user_id                 TEXT NOT NULL,
    merchant_id             TEXT,
    amount                  FLOAT,
    amount_ratio_10txn      FLOAT,
    amount_zscore_10txn     FLOAT,
    amount_vs_30txn_mean    FLOAT,
    velocity_1h             INT,
    velocity_24h            INT,
    new_device_flag         INT,
    card_present            INT,
    cross_border_flag       INT,
    geo_anomaly_score       FLOAT,
    hour_probability        FLOAT,
    merchant_risk_score     FLOAT,
    rule_based_score        FLOAT,
    hour_of_day             INT,
    day_of_week             INT,
    is_fraud                BOOLEAN,
    fraud_score             FLOAT,
    feature_version         TEXT DEFAULT 'v2.1',
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_txn_user_id ON transaction_features(user_id);
CREATE INDEX IF NOT EXISTS idx_txn_created ON transaction_features(created_at);
CREATE INDEX IF NOT EXISTS idx_txn_is_fraud ON transaction_features(is_fraud);

-- Drift monitoring snapshots
CREATE TABLE IF NOT EXISTS drift_reports (
    id              SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMPTZ DEFAULT NOW(),
    features_checked INT,
    features_drifted INT,
    drift_rate      FLOAT,
    report_json     JSONB,
    action_taken    TEXT
);

-- Model registry
CREATE TABLE IF NOT EXISTS model_registry (
    id              SERIAL PRIMARY KEY,
    version         TEXT NOT NULL UNIQUE,
    trained_at      TIMESTAMPTZ DEFAULT NOW(),
    n_samples       INT,
    fraud_rate      FLOAT,
    roc_auc         FLOAT,
    threshold       FLOAT,
    is_active       BOOLEAN DEFAULT FALSE,
    metadata        JSONB
);

-- Alert log
CREATE TABLE IF NOT EXISTS alert_log (
    id          SERIAL PRIMARY KEY,
    alert_type  TEXT,
    severity    TEXT,
    message     TEXT,
    resolved    BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO feast;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO feast;
