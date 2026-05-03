# Payment Operations Monitoring and AI-powered Alerting System

## Overview

This is a production-ready Real-time Payment Operations Monitoring and AI-powered Alerting System. It ingests high-volume payment event streams, detects anomalies in real-time, auto-generates incident RCAs using LLM, and delivers intelligent alerts.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Synthetic     │────▶│    Kafka         │────▶│   Stream        │
│   Generator     │     │  (payment.events)│     │   Processor     │
│   (1000 TPS)    │     │                  │     │                 │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
┌─────────────────┐     ┌──────────────────┐     ┌────────▼────────┐
│   PagerDuty/    │◀────│   Alerting       │◀────│   Anomaly       │
│   Slack         │     │   Service        │     │   Detector      │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
┌─────────────────┐     ┌──────────────────┐     ┌────────▼────────┐
│   Grafana       │◀────│   ClickHouse     │◀────│   LLM Analysis  │
│   Dashboards    │     │   Time-series DB │     │   (Claude/OpenAI)│
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Go 1.21+ (for local development)

### One-Command Deployment

```bash
# Start the entire stack
docker-compose up -d

# View logs
docker-compose logs -f

# Stop everything
docker-compose down
```

### Services Started

| Service | Port | Description |
|---------|------|-------------|
| Synthetic Generator | 8080 | Generates payment events at configurable TPS |
| Kafka | 9092 | Message broker for event streaming |
| ClickHouse | 8123 (HTTP), 9000 (Native) | Time-series database |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Dashboards (admin/admin123) |
| Anomaly Detector | 8081 | Detection and alerting service |

## Activating Chaos Scenarios

The system includes built-in chaos engineering scenarios to test alerting:

### 1. Gateway Failure Scenario
```bash
curl -X POST http://localhost:8080/scenarios/activate \
  -H "Content-Type: application/json" \
  -d '{
    "type": "gateway_failure",
    "gateway": "HDFC",
    "duration_seconds": 300
  }'
```

### 2. Latency Spike Scenario
```bash
curl -X POST http://localhost:8080/scenarios/activate \
  -H "Content-Type: application/json" \
  -d '{
    "type": "latency_spike",
    "gateway": "ICICI",
    "duration_seconds": 180,
    "parameters": {"spike_probability": 0.8}
  }'
```

### 3. Success Rate Drop Scenario
```bash
curl -X POST http://localhost:8080/scenarios/activate \
  -H "Content-Type: application/json" \
  -d '{
    "type": "success_drop",
    "duration_seconds": 300,
    "parameters": {"failure_rate": 0.4}
  }'
```

### 4. Fraud Spike Scenario
```bash
curl -X POST http://localhost:8080/scenarios/activate \
  -H "Content-Type: application/json" \
  -d '{
    "type": "fraud_spike",
    "merchant_id": "merchant_001",
    "duration_seconds": 120
  }'
```

### Check Current Scenario
```bash
curl http://localhost:8080/scenarios/current
```

### Get Generator Stats
```bash
curl http://localhost:8080/stats
```

## Grafana Dashboards

Access Grafana at http://localhost:3000 (admin/admin123)

### Dashboard 1: Payment Health Overview
- Success rate gauge (color-coded: green >98%, amber 95-98%, red <95%)
- Transaction volume time series
- P95 latency by gateway with SLA line at 300ms
- Failure heatmap (error code × gateway)
- Gateway health summary table

### Dashboard 2: Anomaly Center
- Active anomalies list with severity badges
- Anomaly count over time
- Anomalies by type and severity (pie charts)
- Average resolution time
- False positive rate tracker

## Alert Routing

| Severity | Condition | Channels |
|----------|-----------|----------|
| P0 | success_rate < 90% for >5 mins | PagerDuty (high) + Slack #incidents + SMS |
| P1 | success_rate < 95% for >10 mins | PagerDuty (low) + Slack #incidents |
| P2 | p99_latency > 2s for >15 mins | Slack #alerts |
| P3 | CUSUM detected slow drift | Slack #monitoring |

## Anomaly Detection Methods

The system uses three detection methods and requires 2/3 agreement to reduce false positives:

1. **EWMA (Exponentially Weighted Moving Average)**
   - α = 0.3 smoothing factor
   - Detects sudden spikes/drops vs smoothed baseline

2. **CUSUM (Cumulative Sum Control Chart)**
   - Detects sustained drift (e.g., gradual success rate decline)
   - Threshold = 5.0, Slack = 0.5

3. **Z-Score**
   - Compares to 7-day historical baseline
   - Threshold = 3.0 standard deviations

## LLM Incident Analysis

When a P0 or P1 anomaly is detected, the system:

1. Gathers context from ClickHouse:
   - Last 2 hours of metrics
   - Error code distribution changes
   - Comparison to baseline
   - Similar past anomalies

2. Calls LLM (Claude or OpenAI) with structured prompt

3. Receives analysis including:
   - Root causes (ranked by probability)
   - Immediate actions (prioritized checklist)
   - Impact estimate
   - Escalation recommendation

4. Sends enriched Slack message with action buttons

### Configure LLM

```bash
# For Claude
export LLM_API_KEY="your-claude-api-key"
export LLM_PROVIDER="claude"

# For OpenAI
export LLM_API_KEY="your-openai-api-key"
export LLM_PROVIDER="openai"
```

## Configuring Alerts

### Slack Integration
```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### PagerDuty Integration
```bash
export PAGERDUTY_KEY="your-pagerduty-routing-key"
```

## Local Development

### Build All Services
```bash
cd /workspace
go mod download

# Build generator
go build -o bin/generator ./cmd/synthetic-generator

# Build processor
go build -o bin/processor ./cmd/processor

# Build detector
go build -o bin/detector ./cmd/detector
```

### Run Individual Services

```bash
# Set environment variables
export KAFKA_BROKERS=localhost:9092
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123

# Run generator
./bin/generator

# Run processor (in another terminal)
./bin/processor

# Run detector (in another terminal)
./bin/detector
```

## ClickHouse Queries

### Current Success Rate by Gateway
```sql
SELECT 
    gateway, 
    countIf(status = 'success') * 100.0 / count(*) AS success_rate_pct,
    count(*) AS txn_count
FROM payments.payment_events
WHERE event_time >= now() - INTERVAL 5 MINUTE
GROUP BY gateway
ORDER BY success_rate_pct ASC;
```

### P95 Latency Trend
```sql
SELECT 
    toStartOfMinute(event_time) AS minute,
    gateway,
    quantile(0.95)(latency_ms) AS p95_latency
FROM payments.payment_events
WHERE event_time >= now() - INTERVAL 1 HOUR
GROUP BY minute, gateway
ORDER BY minute ASC;
```

### Error Code Distribution
```sql
SELECT 
    error_code,
    gateway,
    count(*) AS error_count
FROM payments.payment_events
WHERE status != 'success' 
  AND event_time >= now() - INTERVAL 30 MINUTE
GROUP BY error_code, gateway
ORDER BY error_count DESC;
```

## SRE Runbook

### During an Incident

1. **Alert Received** (Slack/PagerDuty)
   - Review the LLM-generated analysis in Slack
   - Click "Acknowledge" button if you're taking ownership

2. **Open Dashboards**
   - Go to Grafana Anomaly Center dashboard
   - Check which gateway/merchant is affected
   - Review the failure heatmap for error patterns

3. **Initial Assessment**
   - Is this affecting all gateways or just one?
   - What's the error code distribution?
   - Compare to baseline (same time last week)

4. **Immediate Actions** (from LLM analysis)
   - Follow the prioritized checklist
   - Common actions:
     - Contact gateway provider if gateway-specific
     - Check internal service health
     - Review recent deployments

5. **Communication**
   - Update Slack thread with findings
   - Escalate to engineering team if needed
   - Notify stakeholders for P0 incidents

6. **Resolution**
   - Monitor recovery in Grafana
   - System auto-detects resolution and sends notification
   - Document root cause in post-mortem

### Common Scenarios

#### Single Gateway Failure
- **Symptoms**: One gateway showing >90% failure rate
- **Action**: Contact gateway provider, route traffic to healthy gateways

#### Latency Spike Across All Gateways
- **Symptoms**: P95 latency >2s across all gateways
- **Action**: Check internal services, network, database connections

#### Gradual Success Rate Decline
- **Symptoms**: CUSUM detects slow drift over 30+ minutes
- **Action**: Review recent changes, check for capacity issues

## Kubernetes Deployment

See `k8s/` directory for Kubernetes manifests:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployments.yaml
kubectl apply -f k8s/services.yaml
```

## Monitoring the Monitor

The system exposes Prometheus metrics:

- `payment_messages_consumed_total` - Messages consumed from Kafka
- `payment_events_buffered` - Events currently buffered
- `payment_flushes_total` - Flush operations to ClickHouse

Access metrics at http://localhost:8080/metrics

## License

MIT License
