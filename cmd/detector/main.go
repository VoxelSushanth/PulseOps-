package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/slack-go/slack"
	"go.uber.org/zap"

	"payment-monitoring/internal/alerting"
	"payment-monitoring/internal/anomaly"
	"payment-monitoring/internal/clickhouse"
	"payment-monitoring/internal/config"
	"payment-monitoring/internal/llm"
	"payment-monitoring/pkg/models"
)

// Detector handles anomaly detection and alerting
type Detector struct {
	config       *config.Config
	logger       *zap.Logger
	chClient     *clickhouse.Client
	anomalyDet   *anomaly.Detector
	alertSvc     *alerting.Service
	llmClient    *llm.Client
	
	kafkaProducer sarama.SyncProducer
	activeAnomalies map[string]*models.AnomalyEvent
	mu              sync.RWMutex
	
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewDetector(cfg *config.Config, logger *zap.Logger) (*Detector, error) {
	// Setup ClickHouse client
	chClient, err := clickhouse.NewClient(&cfg.ClickHouse, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// Setup Kafka producer for anomaly events
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer(cfg.Kafka.Brokers, kafkaConfig)
	if err != nil {
		chClient.Close()
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Setup anomaly detector
	anomalyDet := anomaly.NewDetector(&cfg.Processor)

	// Setup alerting service
	alertSvc := alerting.NewService(&cfg.Alerting, logger)

	// Setup LLM client
	llmClient := llm.NewClient(&cfg.LLM, logger)

	return &Detector{
		config:          cfg,
		logger:          logger,
		chClient:        chClient,
		anomalyDet:      anomalyDet,
		alertSvc:        alertSvc,
		llmClient:       llmClient,
		kafkaProducer:   producer,
		activeAnomalies: make(map[string]*models.AnomalyEvent),
		stopCh:          make(chan struct{}),
	}, nil
}

func (d *Detector) Start(ctx context.Context) error {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runDetectionLoop(ctx)
	}()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runResolutionChecker(ctx)
	}()

	d.logger.Info("anomaly detector started")
	return nil
}

func (d *Detector) Stop() {
	close(d.stopCh)
	d.wg.Wait()
	d.kafkaProducer.Close()
	d.chClient.Close()
	d.logger.Info("anomaly detector stopped")
}

func (d *Detector) runDetectionLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.detectAnomalies(ctx)
		}
	}
}

func (d *Detector) detectAnomalies(ctx context.Context) {
	// Get current gateway health metrics
	gateways := []string{"HDFC", "ICICI", "SBI", "AXIS", "RAZORPAY", "STRIPE", "PAYPAL"}
	
	for _, gateway := range gateways {
		d.checkGatewayMetrics(ctx, gateway)
	}

	// Check overall success rate
	d.checkOverallMetrics(ctx)
}

func (d *Detector) checkGatewayMetrics(ctx context.Context, gateway string) {
	// Get current success rate for gateway
	currentRate, err := d.chClient.GetCurrentSuccessRate(ctx, 5)
	if err != nil {
		d.logger.Error("failed to get gateway success rate", zap.String("gateway", gateway), zap.Error(err))
		return
	}

	// Get baseline from 7 days ago
	baselineMean, baselineStddev, err := d.chClient.GetBaselineForZScore(ctx, "success_rate", "", gateway)
	if err != nil {
		// Use default baseline if historical data not available
		baselineMean = 98.0
		baselineStddev = 2.0
	}

	// Check for anomaly
	anomalyEvent := d.anomalyDet.CheckAnomaly(
		fmt.Sprintf("gateway_%s_success_rate", gateway),
		"success_rate",
		currentRate,
		baselineMean,
		baselineStddev,
		"",
		gateway,
		"",
	)

	if anomalyEvent != nil && !d.anomalyDet.ShouldSuppress(anomalyEvent.ID) {
		d.handleAnomaly(ctx, anomalyEvent)
	}
}

func (d *Detector) checkOverallMetrics(ctx context.Context) {
	// Get overall success rate
	currentRate, err := d.chClient.GetCurrentSuccessRate(ctx, 5)
	if err != nil {
		d.logger.Error("failed to get overall success rate", zap.Error(err))
		return
	}

	// Get baseline
	baselineMean, baselineStddev, err := d.chClient.GetBaselineForZScore(ctx, "success_rate", "", "")
	if err != nil {
		baselineMean = 98.0
		baselineStddev = 1.5
	}

	// Check for anomaly
	anomalyEvent := d.anomalyDet.CheckAnomaly(
		"overall_success_rate",
		"success_rate",
		currentRate,
		baselineMean,
		baselineStddev,
		"",
		"",
		"",
	)

	if anomalyEvent != nil && !d.anomalyDet.ShouldSuppress(anomalyEvent.ID) {
		d.handleAnomaly(ctx, anomalyEvent)
	}
}

func (d *Detector) handleAnomaly(ctx context.Context, anomalyEvent *models.AnomalyEvent) {
	d.logger.Warn("anomaly detected",
		zap.String("id", anomalyEvent.ID),
		zap.String("severity", anomalyEvent.Severity),
		zap.String("metric", anomalyEvent.Metric),
		zap.String("gateway", anomalyEvent.Gateway))

	// Store active anomaly
	d.mu.Lock()
	d.activeAnomalies[anomalyEvent.ID] = anomalyEvent
	d.mu.Unlock()

	// Insert into ClickHouse
	if err := d.chClient.InsertAnomaly(ctx, *anomalyEvent); err != nil {
		d.logger.Error("failed to insert anomaly", zap.Error(err))
	}

	// Publish to Kafka anomaly topic
	data, _ := json.Marshal(anomalyEvent)
	msg := &sarama.ProducerMessage{
		Topic: d.config.Kafka.AnomaliesTopic,
		Key:   sarama.StringEncoder(anomalyEvent.Metric),
		Value: sarama.ByteEncoder(data),
	}
	d.kafkaProducer.SendMessage(msg)

	// Send alert based on severity
	d.sendAlert(ctx, anomalyEvent)

	// Trigger LLM analysis for P0/P1 incidents
	if anomalyEvent.Severity == "P0" || anomalyEvent.Severity == "P1" {
		go d.analyzeWithLLM(ctx, anomalyEvent)
	}
}

func (d *Detector) sendAlert(ctx context.Context, anomalyEvent *models.AnomalyEvent) {
	// Determine channels based on severity
	var channels []string
	switch anomalyEvent.Severity {
	case "P0":
		channels = []string{"pagerduty", "slack", "sms"}
	case "P1":
		channels = []string{"pagerduty", "slack"}
	case "P2":
		channels = []string{"slack"}
	case "P3":
		channels = []string{"slack_monitoring"}
	}

	message := fmt.Sprintf("[%s] Anomaly detected on %s: %.2f (baseline: %.2f, deviation: %.1f%%)",
		anomalyEvent.Severity,
		anomalyEvent.Metric,
		anomalyEvent.CurrentValue,
		anomalyEvent.BaselineValue,
		anomalyEvent.Deviation)

	if anomalyEvent.Gateway != "" {
		message += fmt.Sprintf(" Gateway: %s", anomalyEvent.Gateway)
	}

	for _, channel := range channels {
		if err := d.alertSvc.SendAlert(ctx, channel, message, anomalyEvent); err != nil {
			d.logger.Error("failed to send alert", zap.String("channel", channel), zap.Error(err))
		}
	}
}

func (d *Detector) analyzeWithLLM(ctx context.Context, anomalyEvent *models.AnomalyEvent) {
	// Gather context for LLM
	incidentCtx := llm.IncidentContext{
		Anomaly: *anomalyEvent,
		CurrentMetrics: map[string]float64{
			"success_rate": anomalyEvent.CurrentValue,
		},
		BaselineMetrics: map[string]float64{
			"success_rate": anomalyEvent.BaselineValue,
		},
		TimeWindow: "last_5_minutes",
	}

	if anomalyEvent.Gateway != "" {
		incidentCtx.AffectedGateways = []string{anomalyEvent.Gateway}
	}

	// Get similar past incidents
	similarIncidents, err := d.chClient.GetRecentAnomalies(ctx, 10)
	if err == nil {
		incidentCtx.SimilarIncidents = similarIncidents
	}

	// Call LLM for analysis
	analysis, err := d.llmClient.AnalyzeIncident(ctx, incidentCtx)
	if err != nil {
		d.logger.Error("LLM analysis failed", zap.Error(err))
		return
	}

	d.logger.Info("LLM analysis completed",
		zap.String("escalation", analysis.EscalationLevel),
		zap.Float32("confidence", analysis.ConfidenceScore))

	// Send enriched alert to Slack with LLM analysis
	slackMsg := d.llmClient.GenerateSlackMessage(analysis, anomalyEvent)
	if err := d.alertSvc.SendSlackBlockMessage(ctx, slackMsg); err != nil {
		d.logger.Error("failed to send Slack block message", zap.Error(err))
	}
}

func (d *Detector) runResolutionChecker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.checkResolutions(ctx)
		}
	}
}

func (d *Detector) checkResolutions(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for id, anomalyEvent := range d.activeAnomalies {
		if anomalyEvent.Resolved {
			continue
		}

		// Check if metric has recovered
		isResolved := d.checkMetricRecovery(ctx, anomalyEvent)
		
		if isResolved {
			anomalyEvent.Resolved = true
			now := time.Now()
			anomalyEvent.ResolvedAt = &now
			anomalyEvent.Duration = now.Sub(anomalyEvent.DetectedAt)

			d.logger.Info("anomaly resolved",
				zap.String("id", id),
				zap.Duration("duration", anomalyEvent.Duration))

			// Send resolution notification
			d.sendResolutionNotification(ctx, anomalyEvent)
		}
	}
}

func (d *Detector) checkMetricRecovery(ctx context.Context, anomalyEvent *models.AnomalyEvent) bool {
	// Get current value
	currentRate, err := d.chClient.GetCurrentSuccessRate(ctx, 5)
	if err != nil {
		return false
	}

	// Check if back to normal (>95% success rate for success_rate anomalies)
	if anomalyEvent.Metric == "success_rate" {
		return currentRate > 95.0
	}

	return false
}

func (d *Detector) sendResolutionNotification(ctx context.Context, anomalyEvent *models.AnomalyEvent) {
	message := fmt.Sprintf("[RESOLVED] %s anomaly on %s has recovered after %v. Final deviation: %.1f%%",
		anomalyEvent.Severity,
		anomalyEvent.Metric,
		anomalyEvent.Duration,
		anomalyEvent.Deviation)

	// Send to same channels as original alert
	d.alertSvc.SendAlert(ctx, "slack", message, anomalyEvent)
}

// HTTP API for anomaly management
func (d *Detector) SetupRoutes(r *gin.Engine) {
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "healthy"})
	})

	r.GET("/anomalies/active", func(c *gin.Context) {
		d.mu.RLock()
		defer d.mu.RUnlock()
		c.JSON(200, d.activeAnomalies)
	})

	r.POST("/anomalies/:id/acknowledge", func(c *gin.Context) {
		id := c.Param("id")
		d.mu.Lock()
		if anomalyEvent, ok := d.activeAnomalies[id]; ok {
			anomalyEvent.Acknowledged = true
		}
		d.mu.Unlock()
		c.JSON(200, gin.H{"acknowledged": true})
	})

	r.GET("/stats", func(c *gin.Context) {
		d.mu.RLock()
		activeCount := len(d.activeAnomalies)
		d.mu.RUnlock()
		c.JSON(200, gin.H{
			"active_anomalies": activeCount,
		})
	})
}

func serveHTTP(port int, router http.Handler) {
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}
	log.Printf("Starting HTTP server on :%d", port)
	log.Fatal(server.ListenAndServe())
}

func main() {
	// Setup logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Load config
	cfg := config.Get()

	// Create detector
	detector, err := NewDetector(cfg, logger)
	if err != nil {
		log.Fatalf("Failed to create detector: %v", err)
	}

	// Setup HTTP server
	router := gin.Default()
	detector.SetupRoutes(router)

	// Start detector
	ctx, cancel := context.WithCancel(context.Background())
	if err := detector.Start(ctx); err != nil {
		log.Fatalf("Failed to start detector: %v", err)
	}

	// Start HTTP server in goroutine
	go func() {
		if err := router.Run(":8080"); err != nil {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down...")
	cancel()
	detector.Stop()
}

var _ = slack.MessageOption(func(*slack.MessageRequest) {}) // Use slack import
