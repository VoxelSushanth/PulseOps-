package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"payment-monitoring/internal/config"
	"payment-monitoring/pkg/models"
)

// ScenarioType defines the type of scenario to activate
type ScenarioType string

const (
	ScenarioNormal         ScenarioType = "normal"
	ScenarioLatencySpike   ScenarioType = "latency_spike"
	ScenarioGatewayFailure ScenarioType = "gateway_failure"
	ScenarioFraudSpike     ScenarioType = "fraud_spike"
	ScenarioSuccessDrop    ScenarioType = "success_drop"
)

// ActiveScenario holds the current active scenario
type ActiveScenario struct {
	Type            ScenarioType        `json:"type"`
	Gateway         string              `json:"gateway,omitempty"`
	MerchantID      string              `json:"merchant_id,omitempty"`
	DurationSeconds int                 `json:"duration_seconds"`
	StartTime       time.Time           `json:"start_time"`
	EndTime         time.Time           `json:"end_time"`
	Parameters      map[string]float64  `json:"parameters"`
	Active          bool                `json:"active"`
	mu              sync.RWMutex
}

// Generator handles synthetic payment event generation
type Generator struct {
	config          *config.Config
	producer        sarama.SyncProducer
	logger          *zap.Logger
	currentScenario atomic.Value // stores *ActiveScenario
	tps             int32
	eventCounter    int64
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

var (
	merchants   = []string{"merchant_001", "merchant_002", "merchant_003", "merchant_004", "merchant_005"}
	gateways    = []string{"HDFC", "ICICI", "SBI", "AXIS", "RAZORPAY", "STRIPE", "PAYPAL"}
	methods     = []string{"card", "upi", "netbanking", "wallet", "bnpl"}
	regions     = []string{"US-EAST", "US-WEST", "EU-WEST", "EU-CENTRAL", "AP-SOUTH", "AP-EAST"}
	statuses    = []string{"success", "success", "success", "success", "success", "success", "success", "success", "failed", "pending"}
	errorCodes  = []string{"TIMEOUT", "INSUFFICIENT_FUNDS", "INVALID_CARD", "FRAUD_SUSPECTED", "NETWORK_ERROR", "GATEWAY_ERROR", ""}
)

func NewGenerator(cfg *config.Config, logger *zap.Logger) (*Generator, error) {
	// Setup Kafka producer
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer(cfg.Kafka.Brokers, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	g := &Generator{
		config:   cfg,
		producer: producer,
		logger:   logger,
		tps:      1000,
		stopCh:   make(chan struct{}),
	}

	// Initialize with normal scenario
	g.currentScenario.Store(&ActiveScenario{
		Type:       ScenarioNormal,
		Active:     true,
		Parameters: make(map[string]float64),
	})

	return g, nil
}

func (g *Generator) Start(ctx context.Context) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.generateEvents(ctx)
	}()

	g.logger.Info("synthetic generator started", zap.Int32("tps", g.tps))
}

func (g *Generator) Stop() {
	close(g.stopCh)
	g.wg.Wait()
	g.producer.Close()
	g.logger.Info("synthetic generator stopped")
}

func (g *Generator) generateEvents(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopCh:
			return
		case <-ticker.C:
			currentTPS := atomic.LoadInt32(&g.tps)
			if currentTPS <= 0 {
				continue
			}

			batchSize := currentTPS / 10 // Send in 10 batches per second
			if batchSize < 1 {
				batchSize = 1
			}

			for i := 0; i < 10; i++ {
				select {
				case <-g.stopCh:
					return
				default:
					g.sendBatch(batchSize)
				}
			}
		}
	}
}

func (g *Generator) sendBatch(size int) {
	scenario := g.currentScenario.Load().(*ActiveScenario)

	for i := 0; i < size; i++ {
		event := g.generateEvent(scenario)
		
		data, err := json.Marshal(event)
		if err != nil {
			g.logger.Error("failed to marshal event", zap.Error(err))
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: g.config.Kafka.EventsTopic,
			Key:   sarama.StringEncoder(event.MerchantID),
			Value: sarama.ByteEncoder(data),
		}

		partition, offset, err := g.producer.SendMessage(msg)
		if err != nil {
			g.logger.Error("failed to send message", zap.Error(err))
			continue
		}

		if partition%100 == 0 {
			g.logger.Debug("sent message", 
				zap.Int32("partition", partition),
				zap.Int64("offset", offset))
		}

		atomic.AddInt64(&g.eventCounter, 1)
	}
}

func (g *Generator) generateEvent(scenario *ActiveScenario) models.PaymentEvent {
	now := time.Now()

	// Select base values
	merchant := merchants[rand.Intn(len(merchants))]
	gateway := gateways[rand.Intn(len(gateways))]
	method := methods[rand.Intn(len(methods))]
	region := regions[rand.Intn(len(regions))]

	// Apply scenario modifications
	status, errorCode, latency := g.applyScenario(scenario, merchant, gateway)

	event := models.PaymentEvent{
		EventID:    fmt.Sprintf("evt_%s_%d", generateID(), time.Now().UnixNano()),
		PaymentID:  fmt.Sprintf("pay_%s", generateID()),
		MerchantID: merchant,
		Amount:     generateAmount(),
		Method:     method,
		Status:     status,
		Gateway:    gateway,
		LatencyMs:  latency,
		ErrorCode:  errorCode,
		Timestamp:  now,
		Region:     region,
	}

	return event
}

func (g *Generator) applyScenario(scenario *ActiveScenario, merchant, gateway string) (status, errorCode string, latency uint32) {
	scenario.mu.RLock()
	defer scenario.mu.RUnlock()

	if !scenario.Active {
		return g.normalEvent()
	}

	// Check if scenario applies to this event
	applies := true
	if scenario.Gateway != "" && scenario.Gateway != gateway {
		applies = false
	}
	if scenario.MerchantID != "" && scenario.MerchantID != merchant {
		applies = false
	}

	if !applies {
		return g.normalEvent()
	}

	switch scenario.Type {
	case ScenarioLatencySpike:
		return g.latencySpikeEvent(scenario)
	case ScenarioGatewayFailure:
		return g.gatewayFailureEvent(scenario)
	case ScenarioFraudSpike:
		return g.fraudSpikeEvent(scenario)
	case ScenarioSuccessDrop:
		return g.successDropEvent(scenario)
	default:
		return g.normalEvent()
	}
}

func (g *Generator) normalEvent() (string, string, uint32) {
	status := statuses[rand.Intn(len(statuses))]
	var errorCode string
	var latency uint32

	if status == "failed" {
		errorCode = errorCodes[rand.Intn(len(errorCodes)-1)] // Exclude empty
		latency = uint32(rand.Intn(200) + 100)
	} else {
		latency = uint32(rand.Intn(150) + 50)
	}

	return status, errorCode, latency
}

func (g *Generator) latencySpikeEvent(scenario *ActiveScenario) (string, string, uint32) {
	// 80% chance of high latency
	if rand.Float64() < scenario.Parameters["spike_probability"] {
		return "success", "", uint32(rand.Intn(2000) + 1000) // 1-3 seconds
	}
	return g.normalEvent()
}

func (g *Generator) gatewayFailureEvent(scenario *ActiveScenario) (string, string, uint32) {
	// 90% failure rate during gateway failure
	if rand.Float64() < scenario.Parameters["failure_rate"] {
		return "failed", "GATEWAY_ERROR", uint32(rand.Intn(5000) + 3000) // Timeout
	}
	return g.normalEvent()
}

func (g *Generator) fraudSpikeEvent(scenario *ActiveScenario) (string, string, uint32) {
	// Increased fraud detection
	if rand.Float64() < scenario.Parameters["fraud_rate"] {
		return "failed", "FRAUD_SUSPECTED", uint32(rand.Intn(100) + 50)
	}
	return g.normalEvent()
}

func (g *Generator) successDropEvent(scenario *ActiveScenario) (string, string, uint32) {
	// Reduced success rate
	if rand.Float64() < scenario.Parameters["failure_rate"] {
		errCodes := []string{"TIMEOUT", "NETWORK_ERROR", "GATEWAY_ERROR"}
		return "failed", errCodes[rand.Intn(len(errCodes))], uint32(rand.Intn(500) + 200)
	}
	return g.normalEvent()
}

func (g *Generator) ActivateScenario(scenarioType ScenarioType, gateway, merchantID string, durationSeconds int, params map[string]float64) {
	scenario := &ActiveScenario{
		Type:            scenarioType,
		Gateway:         gateway,
		MerchantID:      merchantID,
		DurationSeconds: durationSeconds,
		StartTime:       time.Now(),
		EndTime:         time.Now().Add(time.Duration(durationSeconds) * time.Second),
		Parameters:      params,
		Active:          true,
	}

	// Set default parameters based on scenario type
	if scenario.Parameters == nil {
		scenario.Parameters = make(map[string]float64)
	}

	switch scenarioType {
	case ScenarioLatencySpike:
		if _, ok := scenario.Parameters["spike_probability"]; !ok {
			scenario.Parameters["spike_probability"] = 0.8
		}
	case ScenarioGatewayFailure:
		if _, ok := scenario.Parameters["failure_rate"]; !ok {
			scenario.Parameters["failure_rate"] = 0.9
		}
	case ScenarioFraudSpike:
		if _, ok := scenario.Parameters["fraud_rate"]; !ok {
			scenario.Parameters["fraud_rate"] = 0.3
		}
	case ScenarioSuccessDrop:
		if _, ok := scenario.Parameters["failure_rate"]; !ok {
			scenario.Parameters["failure_rate"] = 0.4
		}
	}

	g.currentScenario.Store(scenario)

	g.logger.Info("scenario activated",
		zap.String("type", string(scenarioType)),
		zap.String("gateway", gateway),
		zap.Int("duration_seconds", durationSeconds))

	// Schedule deactivation
	go func() {
		time.Sleep(time.Duration(durationSeconds) * time.Second)
		scenario.mu.Lock()
		scenario.Active = false
		scenario.mu.Unlock()
		g.logger.Info("scenario deactivated", zap.String("type", string(scenarioType)))
	}()
}

func (g *Generator) GetCurrentScenario() *ActiveScenario {
	return g.currentScenario.Load().(*ActiveScenario)
}

func (g *Generator) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_events": atomic.LoadInt64(&g.eventCounter),
		"tps":          atomic.LoadInt32(&g.tps),
		"scenario":     g.GetCurrentScenario(),
	}
}

func (g *Generator) SetTPS(tps int32) {
	atomic.StoreInt32(&g.tps, tps)
	g.logger.Info("TPS updated", zap.Int32("tps", tps))
}

// HTTP API for scenario control
func (g *Generator) SetupRoutes(r *gin.Engine) {
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "healthy"})
	})

	r.GET("/stats", func(c *gin.Context) {
		c.JSON(200, g.GetStats())
	})

	r.POST("/scenarios/activate", func(c *gin.Context) {
		var req struct {
			Type            string  `json:"type" binding:"required"`
			Gateway         string  `json:"gateway,omitempty"`
			MerchantID      string  `json:"merchant_id,omitempty"`
			DurationSeconds int     `json:"duration_seconds" binding:"required"`
			Parameters      map[string]float64 `json:"parameters,omitempty"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		scenarioType := ScenarioType(req.Type)
		if req.DurationSeconds <= 0 {
			req.DurationSeconds = 300 // Default 5 minutes
		}

		g.ActivateScenario(scenarioType, req.Gateway, req.MerchantID, req.DurationSeconds, req.Parameters)
		c.JSON(200, gin.H{
			"message": "scenario activated",
			"type":    req.Type,
			"duration_seconds": req.DurationSeconds,
		})
	})

	r.GET("/scenarios/current", func(c *gin.Context) {
		c.JSON(200, g.GetCurrentScenario())
	})

	r.POST("/scenarios/deactivate", func(c *gin.Context) {
		scenario := g.GetCurrentScenario()
		scenario.mu.Lock()
		scenario.Active = false
		scenario.mu.Unlock()
		c.JSON(200, gin.H{"message": "scenario deactivated"})
	})

	r.POST("/tps/set", func(c *gin.Context) {
		var req struct {
			TPS int32 `json:"tps" binding:"required"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		g.SetTPS(req.TPS)
		c.JSON(200, gin.H{"tps": req.TPS})
	})
}

func generateID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, 12)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func generateAmount() float64 {
	// Generate realistic payment amounts
	amounts := []float64{99, 199, 299, 499, 999, 1499, 2999, 4999, 9999, 19999}
	base := amounts[rand.Intn(len(amounts))]
	// Add some variance
	variance := float64(rand.Intn(100)) / 100.0
	return base + variance
}

func main() {
	// Setup logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Load config
	cfg := config.Get()

	// Seed random
	rand.Seed(time.Now().UnixNano())

	// Create generator
	generator, err := NewGenerator(cfg, logger)
	if err != nil {
		log.Fatalf("Failed to create generator: %v", err)
	}

	// Setup HTTP server for control
	router := gin.Default()
	generator.SetupRoutes(router)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// Start generator
	ctx, cancel := context.WithCancel(context.Background())
	generator.Start(ctx)

	// Start HTTP server in goroutine
	go func() {
		logger.Info("starting HTTP control server on :8080")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down...")
	cancel()
	generator.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	httpServer.Shutdown(shutdownCtx)
}

// Buffer pool for JSON encoding
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var _ = bufferPool.Get // Use bufferPool variable
