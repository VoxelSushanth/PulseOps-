package config

import (
	"encoding/json"
	"os"
	"time"
)

// Config holds all configuration for the system
type Config struct {
	Kafka      KafkaConfig      `json:"kafka"`
	ClickHouse ClickHouseConfig `json:"clickhouse"`
	Prometheus PrometheusConfig `json:"prometheus"`
	Alerting   AlertingConfig   `json:"alerting"`
	LLM        LLMConfig        `json:"llm"`
	Processor  ProcessorConfig  `json:"processor"`
}

type KafkaConfig struct {
	Brokers        []string `json:"brokers"`
	ConsumerGroup  string   `json:"consumer_group"`
	EventsTopic    string   `json:"events_topic"`
	AnomaliesTopic string   `json:"anomalies_topic"`
	Timeout        int      `json:"timeout_seconds"`
}

type ClickHouseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type PrometheusConfig struct {
	Enabled bool   `json:"enabled"`
	Port    int    `json:"port"`
	Path    string `json:"path"`
}

type AlertingConfig struct {
	PagerDutyKey     string            `json:"pagerduty_key"`
	SlackWebhookURL  string            `json:"slack_webhook_url"`
	SlackChannel     string            `json:"slack_channel"`
	DedupWindowMins  int               `json:"dedup_window_mins"`
	AutoResolveMins  int               `json:"auto_resolve_mins"`
	SeverityRules    SeverityRules     `json:"severity_rules"`
}

type SeverityRules struct {
	P0 SuccessRateRule `json:"p0"`
	P1 SuccessRateRule `json:"p1"`
	P2 LatencyRule     `json:"p2"`
	P3 TrendRule       `json:"p3"`
}

type SuccessRateRule struct {
	Threshold float64 `json:"threshold"`
	DurationMins int  `json:"duration_mins"`
}

type LatencyRule struct {
	ThresholdMs int `json:"threshold_ms"`
	DurationMins int `json:"duration_mins"`
}

type TrendRule struct {
	MinCUSUM float64 `json:"min_cusum"`
}

type LLMConfig struct {
	Provider     string `json:"provider"` // claude, openai
	APIKey       string `json:"api_key"`
	Model        string `json:"model"`
	MaxTokens    int    `json:"max_tokens"`
	Temperature  float64 `json:"temperature"`
	EndpointURL  string `json:"endpoint_url,omitempty"`
}

type ProcessorConfig struct {
	WindowSizeSeconds     int           `json:"window_size_seconds"`
	FlushIntervalSeconds  int           `json:"flush_interval_seconds"`
	EWMASmoothingFactor   float64       `json:"ewma_alpha"`
	CUSUMThreshold        float64       `json:"cusum_threshold"`
	ZScoreThreshold       float64       `json:"zscore_threshold"`
	AnomalyAgreementCount int           `json:"anomaly_agreement_count"`
	BaselineDays          int           `json:"baseline_days"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:        []string{"localhost:9092"},
			ConsumerGroup:  "payment-processor-group",
			EventsTopic:    "payment.events",
			AnomaliesTopic: "payment.anomalies",
			Timeout:        30,
		},
		ClickHouse: ClickHouseConfig{
			Host:     "localhost",
			Port:     8123,
			Database: "payments",
			Username: "default",
			Password: "",
		},
		Prometheus: PrometheusConfig{
			Enabled: true,
			Port:    9090,
			Path:    "/metrics",
		},
		Alerting: AlertingConfig{
			PagerDutyKey:    "",
			SlackWebhookURL: "",
			SlackChannel:    "#incidents",
			DedupWindowMins: 30,
			AutoResolveMins: 15,
			SeverityRules: SeverityRules{
				P0: SuccessRateRule{Threshold: 0.90, DurationMins: 5},
				P1: SuccessRateRule{Threshold: 0.95, DurationMins: 10},
				P2: LatencyRule{ThresholdMs: 2000, DurationMins: 15},
				P3: TrendRule{MinCUSUM: 5.0},
			},
		},
		LLM: LLMConfig{
			Provider:    "claude",
			APIKey:      "",
			Model:       "claude-3-sonnet-20240229",
			MaxTokens:   2048,
			Temperature: 0.3,
		},
		Processor: ProcessorConfig{
			WindowSizeSeconds:     60,
			FlushIntervalSeconds:  30,
			EWMASmoothingFactor:   0.3,
			CUSUMThreshold:        5.0,
			ZScoreThreshold:       3.0,
			AnomalyAgreementCount: 2,
			BaselineDays:          7,
		},
	}
}

// LoadConfig loads configuration from a JSON file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return DefaultConfig(), err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// FromEnv loads configuration from environment variables
func FromEnv() *Config {
	cfg := DefaultConfig()

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Kafka.Brokers = splitString(brokers, ",")
	}
	if group := os.Getenv("KAFKA_CONSUMER_GROUP"); group != "" {
		cfg.Kafka.ConsumerGroup = group
	}
	if chHost := os.Getenv("CLICKHOUSE_HOST"); chHost != "" {
		cfg.ClickHouse.Host = chHost
	}
	if chPass := os.Getenv("CLICKHOUSE_PASSWORD"); chPass != "" {
		cfg.ClickHouse.Password = chPass
	}
	if slackURL := os.Getenv("SLACK_WEBHOOK_URL"); slackURL != "" {
		cfg.Alerting.SlackWebhookURL = slackURL
	}
	if pdKey := os.Getenv("PAGERDUTY_KEY"); pdKey != "" {
		cfg.Alerting.PagerDutyKey = pdKey
	}
	if llmKey := os.Getenv("LLM_API_KEY"); llmKey != "" {
		cfg.LLM.APIKey = llmKey
	}
	if llmProvider := os.Getenv("LLM_PROVIDER"); llmProvider != "" {
		cfg.LLM.Provider = llmProvider
	}

	return cfg
}

func splitString(s, sep string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
		}
	}
	result = append(result, s[start:])
	return result
}

// Get returns config with sensible defaults, overridden by env vars
func Get() *Config {
	return FromEnv()
}

var _ = time.Second // import time for potential use
