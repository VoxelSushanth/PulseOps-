package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"payment-monitoring/internal/config"
	"payment-monitoring/pkg/models"
)

// Service handles multi-channel alerting
type Service struct {
	config     *config.AlertingConfig
	logger     *zap.Logger
	httpClient *http.Client
	
	// Deduplication tracking
	alertHistory map[string]time.Time
	historyMu    sync.RWMutex
}

// NewService creates a new alerting service
func NewService(cfg *config.AlertingConfig, logger *zap.Logger) *Service {
	return &Service{
		config:       cfg,
		logger:       logger,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		alertHistory: make(map[string]time.Time),
	}
}

// SendAlert sends an alert to the specified channel
func (s *Service) SendAlert(ctx context.Context, channel, message string, anomaly *models.AnomalyEvent) error {
	// Check deduplication
	if s.isDuplicate(anomaly.ID) {
		s.logger.Debug("skipping duplicate alert", zap.String("id", anomaly.ID))
		return nil
	}

	switch channel {
	case "slack":
		return s.sendSlackMessage(ctx, message)
	case "slack_monitoring":
		return s.sendSlackMessageToChannel(ctx, message, "#monitoring")
	case "pagerduty":
		return s.sendPagerDutyIncident(ctx, message, anomaly)
	case "sms":
		return s.sendSMS(ctx, message)
	default:
		return fmt.Errorf("unknown channel: %s", channel)
	}
}

func (s *Service) isDuplicate(alertID string) bool {
	s.historyMu.RLock()
	defer s.historyMu.RUnlock()

	lastAlerted, ok := s.alertHistory[alertID]
	if !ok {
		return false
	}

	// Check if within dedup window
	return time.Since(lastAlerted) < time.Duration(s.config.DedupWindowMins)*time.Minute
}

func (s *Service) recordAlert(alertID string) {
	s.historyMu.Lock()
	defer s.historyMu.Unlock()
	s.alertHistory[alertID] = time.Now()
}

func (s *Service) sendSlackMessage(ctx context.Context, message string) error {
	if s.config.SlackWebhookURL == "" {
		s.logger.Warn("Slack webhook URL not configured, skipping")
		return nil
	}

	payload := map[string]string{
		"text":        message,
		"channel":     s.config.SlackChannel,
		"username":    "Payment Monitor",
		"icon_emoji":  ":warning:",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.config.SlackWebhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Slack API returned status %d", resp.StatusCode)
	}

	s.logger.Info("Slack alert sent", zap.String("message", message))
	return nil
}

func (s *Service) sendSlackMessageToChannel(ctx context.Context, message, channel string) error {
	// Clone config with different channel
	originalChannel := s.config.SlackChannel
	s.config.SlackChannel = channel
	defer func() { s.config.SlackChannel = originalChannel }()

	return s.sendSlackMessage(ctx, message)
}

// SendSlackBlockMessage sends a rich Slack block kit message
func (s *Service) SendSlackBlockMessage(ctx context.Context, blocks map[string]interface{}) error {
	if s.config.SlackWebhookURL == "" {
		return nil
	}

	jsonData, err := json.Marshal(blocks)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.config.SlackWebhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (s *Service) sendPagerDutyIncident(ctx context.Context, message string, anomaly *models.AnomalyEvent) error {
	if s.config.PagerDutyKey == "" {
		s.logger.Warn("PagerDuty key not configured, skipping")
		return nil
	}

	// Map severity to PagerDuty urgency
	urgency := "low"
	if anomaly.Severity == "P0" {
		urgency = "high"
	}

	payload := map[string]interface{}{
		"routing_key": s.config.PagerDutyKey,
		"event_action": "trigger",
		"dedup_key": anomaly.ID,
		"payload": map[string]interface{}{
			"summary":   message,
			"source":    "payment-monitoring",
			"severity":  s.mapSeverityToPagerDuty(anomaly.Severity),
			"timestamp": time.Now().Format(time.RFC3339),
			"class":     anomaly.Metric,
			"component": anomaly.Gateway,
			"custom_details": map[string]interface{}{
				"current_value":  anomaly.CurrentValue,
				"baseline_value": anomaly.BaselineValue,
				"deviation":      anomaly.Deviation,
				"anomaly_type":   anomaly.Type,
			},
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", 
		"https://events.pagerduty.com/v2/enqueue", 
		bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	s.logger.Info("PagerDuty incident created", zap.String("dedup_key", anomaly.ID))
	return nil
}

func (s *Service) mapSeverityToPagerDuty(severity string) string {
	switch severity {
	case "P0":
		return "critical"
	case "P1":
		return "error"
	case "P2":
		return "warning"
	default:
		return "info"
	}
}

func (s *Service) sendSMS(ctx context.Context, message string) error {
	// SMS integration would go here (Twilio, SNS, etc.)
	// For now, just log it
	s.logger.Info("SMS alert (simulated)", zap.String("message", message))
	return nil
}

// ResolveIncident sends resolution notification to PagerDuty
func (s *Service) ResolveIncident(ctx context.Context, anomalyID string) error {
	if s.config.PagerDutyKey == "" {
		return nil
	}

	payload := map[string]interface{}{
		"routing_key":  s.config.PagerDutyKey,
		"event_action": "resolve",
		"dedup_key":    anomalyID,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		"https://events.pagerduty.com/v2/enqueue",
		bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

var _ = sync.RWMutex{} // Import sync
