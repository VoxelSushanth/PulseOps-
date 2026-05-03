package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"payment-monitoring/internal/clickhouse"
	"payment-monitoring/internal/config"
	"payment-monitoring/pkg/models"
)

var (
	messagesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "payment_messages_consumed_total",
		Help: "Total number of payment messages consumed",
	})
	eventsBuffered = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "payment_events_buffered",
		Help: "Number of events currently buffered",
	})
	flushCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "payment_flushes_total",
		Help: "Total number of flush operations to ClickHouse",
	})
)

func init() {
	prometheus.MustRegister(messagesConsumed, eventsBuffered, flushCount)
}

// Processor handles stream processing of payment events
type Processor struct {
	config        *config.Config
	consumerGroup sarama.ConsumerGroup
	logger        *zap.Logger
	chClient      *clickhouse.Client
	
	eventBuffer   []models.PaymentEvent
	bufferMu      sync.Mutex
	windowStart   time.Time
	
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewProcessor(cfg *config.Config, logger *zap.Logger) (*Processor, error) {
	// Setup Kafka consumer group
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_8_0_0
	kafkaConfig.Consumer.Group.InitialOffset = sarama.OffsetOldest
	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = true
	kafkaConfig.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.ConsumerGroup, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Setup ClickHouse client
	chClient, err := clickhouse.NewClient(&cfg.ClickHouse, logger)
	if err != nil {
		consumerGroup.Close()
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	return &Processor{
		config:        cfg,
		consumerGroup: consumerGroup,
		logger:        logger,
		chClient:      chClient,
		eventBuffer:   make([]models.PaymentEvent, 0, 10000),
		windowStart:   time.Now(),
		stopCh:        make(chan struct{}),
	}, nil
}

func (p *Processor) Start(ctx context.Context) error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.runConsumer(ctx)
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.runFlusher(ctx)
	}()

	p.logger.Info("stream processor started")
	return nil
}

func (p *Processor) Stop() {
	close(p.stopCh)
	p.wg.Wait()
	p.consumerGroup.Close()
	p.chClient.Close()
	p.logger.Info("stream processor stopped")
}

func (p *Processor) runConsumer(ctx context.Context) {
	handler := &consumerHandler{
		processor: p,
		logger:    p.logger,
		ready:     make(chan bool),
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		default:
			if err := p.consumerGroup.Consume(ctx, []string{p.config.Kafka.EventsTopic}, handler); err != nil {
				p.logger.Error("consumer group error", zap.Error(err))
			}
		}
	}
}

func (p *Processor) runFlusher(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.config.Processor.FlushIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			// Final flush
			p.flushBuffer(ctx)
			return
		case <-ticker.C:
			p.flushBuffer(ctx)
		}
	}
}

func (p *Processor) flushBuffer(ctx context.Context) {
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	if len(p.eventBuffer) == 0 {
		return
	}

	events := p.eventBuffer
	p.eventBuffer = make([]models.PaymentEvent, 0, 10000)

	go func() {
		if err := p.chClient.InsertEvents(ctx, events); err != nil {
			p.logger.Error("failed to insert events", zap.Error(err), zap.Int("count", len(events)))
		} else {
			p.logger.Debug("flushed events to ClickHouse", zap.Int("count", len(events)))
			flushCount.Inc()
		}
	}()
}

func (p *Processor) addEvent(event models.PaymentEvent) {
	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	p.eventBuffer = append(p.eventBuffer, event)
	eventsBuffered.Set(float64(len(p.eventBuffer)))
	messagesConsumed.Inc()
}

// consumerHandler implements sarama.ConsumerGroupHandler
type consumerHandler struct {
	processor *Processor
	logger    *zap.Logger
	ready     chan bool
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaims) error {
	for msg := range claim.Messages() {
		var event models.PaymentEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			h.logger.Error("failed to unmarshal message", zap.Error(err))
			continue
		}

		h.processor.addEvent(event)

		// Mark message as processed
		sess.MarkMessage(msg, "")
	}

	return nil
}

func serveMetrics(port int) {
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Starting metrics server on :%d", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func main() {
	// Setup logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Load config
	cfg := config.Get()

	// Create processor
	processor, err := NewProcessor(cfg, logger)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	// Start metrics server
	go serveMetrics(8080)

	// Start processor
	ctx, cancel := context.WithCancel(context.Background())
	if err := processor.Start(ctx); err != nil {
		log.Fatalf("Failed to start processor: %v", err)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down...")
	cancel()
	processor.Stop()
}
