-- ClickHouse Schema for Payment Monitoring System
-- Run this script to initialize the database

-- Create database
CREATE DATABASE IF NOT EXISTS payments;

-- Main events table - stores raw payment events
CREATE TABLE IF NOT EXISTS payments.payment_events (
    event_id String,
    payment_id String,
    merchant_id LowCardinality(String),
    amount Float64,
    method LowCardinality(String),
    status LowCardinality(String),
    gateway LowCardinality(String),
    latency_ms UInt32,
    error_code LowCardinality(String),
    event_time DateTime,
    region LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (merchant_id, gateway, event_time)
TTL event_time + INTERVAL 30 DAY;

-- 1-minute aggregates table
CREATE TABLE IF NOT EXISTS payments.payment_aggregates_1m (
    window_start DateTime,
    merchant_id String,
    gateway String,
    method String,
    success_rate Float32,
    failure_rate Float32,
    txn_count UInt32,
    total_amount Float64,
    p50_latency UInt32,
    p95_latency UInt32,
    p99_latency UInt32,
    anomaly_score Float32,
    error_code_5xx UInt32,
    error_code_4xx UInt32,
    error_code_timeout UInt32,
    error_code_fraud UInt32
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, merchant_id, gateway, method);

-- Anomaly events table
CREATE TABLE IF NOT EXISTS payments.anomaly_events (
    id String,
    type LowCardinality(String),
    severity LowCardinality(String),
    metric LowCardinality(String),
    merchant_id String,
    gateway String,
    method String,
    current_value Float64,
    baseline_value Float64,
    deviation Float64,
    detected_at DateTime,
    duration_seconds UInt32,
    context String, -- JSON string
    acknowledged Boolean DEFAULT false,
    resolved Boolean DEFAULT false,
    resolved_at Nullable(DateTime)
) ENGINE = ReplacingMergeTree()
ORDER BY (detected_at, severity, metric)
TTL detected_at + INTERVAL 90 DAY;

-- Merchant metrics summary (for quick lookups)
CREATE TABLE IF NOT EXISTS payments.merchant_summary (
    merchant_id String,
    date Date,
    hour UInt8,
    total_txn UInt32,
    success_rate Float32,
    gmv Float64,
    avg_latency Float32,
    top_error_code String
) ENGINE = SummingMergeTree()
ORDER BY (merchant_id, date, hour)
TTL date + INTERVAL 30 DAY;

-- Gateway health metrics
CREATE TABLE IF NOT EXISTS payments.gateway_health (
    gateway String,
    window_start DateTime,
    success_rate Float32,
    txn_count UInt32,
    p50_latency UInt32,
    p95_latency UInt32,
    p99_latency UInt32,
    timeout_rate Float32,
    error_rate Float32
) ENGINE = SummingMergeTree()
ORDER BY (gateway, window_start)
TTL window_start + INTERVAL 7 DAY;

-- Materialized view for real-time gateway health
CREATE MATERIALIZED VIEW IF NOT EXISTS payments.gateway_health_mv
TO payments.gateway_health
AS SELECT 
    gateway,
    toStartOfMinute(event_time) AS window_start,
    countIf(status = 'success') * 1.0 / count(*) AS success_rate,
    count(*) AS txn_count,
    quantile(0.5)(latency_ms) AS p50_latency,
    quantile(0.95)(latency_ms) AS p95_latency,
    quantile(0.99)(latency_ms) AS p99_latency,
    countIf(error_code = 'TIMEOUT') * 1.0 / count(*) AS timeout_rate,
    countIf(status != 'success') * 1.0 / count(*) AS error_rate
FROM payments.payment_events
GROUP BY gateway, window_start;

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_merchant_status 
ON payments.payment_events merchant_id, status TYPE bloom_filter GRANULARITY 4;

CREATE INDEX IF NOT EXISTS idx_gateway_latency 
ON payments.payment_events gateway, latency_ms TYPE minmax GRANULARITY 4;
