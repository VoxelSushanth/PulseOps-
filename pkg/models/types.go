package models

import "time"

// PaymentEvent represents a single payment transaction event
type PaymentEvent struct {
	EventID     string    `json:"event_id"`
	PaymentID   string    `json:"payment_id"`
	MerchantID  string    `json:"merchant_id"`
	Amount      float64   `json:"amount"`
	Method      string    `json:"method"`
	Status      string    `json:"status"`
	Gateway     string    `json:"gateway"`
	LatencyMs   uint32    `json:"latency_ms"`
	ErrorCode   string    `json:"error_code"`
	Timestamp   time.Time `json:"timestamp"`
	Region      string    `json:"region"`
}

// AggregateMetrics represents 1-minute aggregated metrics
type AggregateMetrics struct {
	WindowStart  time.Time `ch:"window_start"`
	MerchantID   string    `ch:"merchant_id"`
	Gateway      string    `ch:"gateway"`
	Method       string    `ch:"method"`
	SuccessRate  float32   `ch:"success_rate"`
	FailureRate  float32   `ch:"failure_rate"`
	TxnCount     uint32    `ch:"txn_count"`
	TotalAmount  float64   `ch:"total_amount"`
	P50Latency   uint32    `ch:"p50_latency"`
	P95Latency   uint32    `ch:"p95_latency"`
	P99Latency   uint32    `ch:"p99_latency"`
	AnomalyScore float32   `ch:"anomaly_score"`
}

// AnomalyEvent represents a detected anomaly
type AnomalyEvent struct {
	ID            string                 `json:"id"`
	Type          string                 `json:"type"` // ewma, cusum, zscore
	Severity      string                 `json:"severity"` // P0, P1, P2, P3
	Metric        string                 `json:"metric"`
	MerchantID    string                 `json:"merchant_id,omitempty"`
	Gateway       string                 `json:"gateway,omitempty"`
	Method        string                 `json:"method,omitempty"`
	CurrentValue  float64                `json:"current_value"`
	BaselineValue float64                `json:"baseline_value"`
	Deviation     float64                `json:"deviation"`
	DetectedAt    time.Time              `json:"detected_at"`
	Duration      time.Duration          `json:"duration,omitempty"`
	Context       map[string]interface{} `json:"context,omitempty"`
	Acknowledged  bool                   `json:"acknowledged"`
	Resolved      bool                   `json:"resolved"`
	ResolvedAt    *time.Time             `json:"resolved_at,omitempty"`
}

// AlertConfig holds alerting configuration
type AlertConfig struct {
	Severity         string        `json:"severity"`
	Threshold        float64       `json:"threshold"`
	Duration         time.Duration `json:"duration"`
	PagerDutyEnabled bool          `json:"pagerduty_enabled"`
	SlackEnabled     bool          `json:"slack_enabled"`
	SMSEnabled       bool          `json:"sms_enabled"`
}

// IncidentAnalysis is the LLM-generated analysis
type IncidentAnalysis struct {
	RootCauses           []string `json:"root_causes"`
	ImmediateActions     []string `json:"immediate_actions"`
	ImpactEstimate       string   `json:"impact_estimate"`
	SimilarIncidents     []string `json:"similar_incidents"`
	EscalationLevel      string   `json:"escalation_level"`
	ConfidenceScore      float32  `json:"confidence_score"`
	GeneratedAt          time.Time `json:"generated_at"`
}

// ScenarioConfig for synthetic data generation
type ScenarioConfig struct {
	Type            string            `json:"type"` // normal, latency_spike, gateway_failure, fraud_spike
	Gateway         string            `json:"gateway,omitempty"`
	MerchantID      string            `json:"merchant_id,omitempty"`
	DurationSeconds int               `json:"duration_seconds"`
	StartTime       time.Time         `json:"start_time"`
	Parameters      map[string]float64 `json:"parameters"`
}
