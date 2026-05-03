package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"payment-monitoring/internal/config"
	"payment-monitoring/pkg/models"
)

// Client wraps ClickHouse connection and operations
type Client struct {
	conn   clickhouse.Conn
	config *config.ClickHouseConfig
	logger *zap.Logger
}

// NewClient creates a new ClickHouse client
func NewClient(cfg *config.ClickHouseConfig, logger *zap.Logger) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	logger.Info("connected to ClickHouse", 
		zap.String("host", cfg.Host), 
		zap.Int("port", cfg.Port))

	return &Client{
		conn:   conn,
		config: cfg,
		logger: logger,
	}, nil
}

// InsertEvents inserts a batch of payment events
func (c *Client) InsertEvents(ctx context.Context, events []models.PaymentEvent) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO payments.payment_events")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		if err := batch.Append(
			event.EventID,
			event.PaymentID,
			event.MerchantID,
			event.Amount,
			event.Method,
			event.Status,
			event.Gateway,
			event.LatencyMs,
			event.ErrorCode,
			event.Timestamp,
			event.Region,
		); err != nil {
			return fmt.Errorf("failed to append event: %w", err)
		}
	}

	return batch.Send()
}

// InsertAggregates inserts aggregated metrics
func (c *Client) InsertAggregates(ctx context.Context, aggregates []models.AggregateMetrics) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO payments.payment_aggregates_1m")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, agg := range aggregates {
		if err := batch.Append(
			agg.WindowStart,
			agg.MerchantID,
			agg.Gateway,
			agg.Method,
			agg.SuccessRate,
			agg.FailureRate,
			agg.TxnCount,
			agg.TotalAmount,
			agg.P50Latency,
			agg.P95Latency,
			agg.P99Latency,
			agg.AnomalyScore,
			0, // error_code_5xx
			0, // error_code_4xx
			0, // error_code_timeout
			0, // error_code_fraud
		); err != nil {
			return fmt.Errorf("failed to append aggregate: %w", err)
		}
	}

	return batch.Send()
}

// InsertAnomaly inserts an anomaly event
func (c *Client) InsertAnomaly(ctx context.Context, anomaly models.AnomalyEvent) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO payments.anomaly_events")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	contextJSON := "{}"
	if anomaly.Context != nil {
		// Simple JSON serialization - in production use proper JSON marshal
		contextJSON = "{\"metric\":\"" + anomaly.Metric + "\"}"
	}

	var resolvedAt *time.Time
	if anomaly.ResolvedAt != nil {
		resolvedAt = anomaly.ResolvedAt
	}

	if err := batch.Append(
		anomaly.ID,
		anomaly.Type,
		anomaly.Severity,
		anomaly.Metric,
		anomaly.MerchantID,
		anomaly.Gateway,
		anomaly.Method,
		anomaly.CurrentValue,
		anomaly.BaselineValue,
		anomaly.Deviation,
		anomaly.DetectedAt,
		uint32(anomaly.Duration.Seconds()),
		contextJSON,
		anomaly.Acknowledged,
		anomaly.Resolved,
		resolvedAt,
	); err != nil {
		return fmt.Errorf("failed to append anomaly: %w", err)
	}

	return batch.Send()
}

// GetGatewayHealth retrieves gateway health metrics for the last N minutes
func (c *Client) GetGatewayHealth(ctx context.Context, minutes int) ([]map[string]interface{}, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT 
			gateway,
			toStartOfMinute(event_time) AS minute,
			countIf(status = 'success') * 100.0 / count(*) AS success_rate,
			count(*) AS txn_count,
			quantile(0.95)(latency_ms) AS p95_latency
		FROM payments.payment_events
		WHERE event_time >= now() - INTERVAL ? MINUTE
		GROUP BY gateway, minute
		ORDER BY minute ASC, gateway ASC
	`, minutes)
	if err != nil {
		return nil, fmt.Errorf("failed to query gateway health: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var gateway string
		var minute time.Time
		var successRate float64
		var txnCount uint64
		var p95Latency uint32

		if err := rows.Scan(&gateway, &minute, &successRate, &txnCount, &p95Latency); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		results = append(results, map[string]interface{}{
			"gateway":       gateway,
			"minute":        minute,
			"success_rate":  successRate,
			"txn_count":     txnCount,
			"p95_latency":   p95Latency,
		})
	}

	return results, nil
}

// GetMerchantMetrics retrieves metrics for a specific merchant
func (c *Client) GetMerchantMetrics(ctx context.Context, merchantID string, hours int) ([]map[string]interface{}, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT 
			toStartOfMinute(event_time) AS minute,
			count(*) AS txn_count,
			countIf(status = 'success') * 100.0 / count(*) AS success_rate,
			sum(amount) AS gmv,
			quantile(0.95)(latency_ms) AS p95_latency
		FROM payments.payment_events
		WHERE merchant_id = ? AND event_time >= now() - INTERVAL ? HOUR
		GROUP BY minute
		ORDER BY minute ASC
	`, merchantID, hours)
	if err != nil {
		return nil, fmt.Errorf("failed to query merchant metrics: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var minute time.Time
		var txnCount uint64
		var successRate float64
		var gmv float64
		var p95Latency uint32

		if err := rows.Scan(&minute, &txnCount, &successRate, &gmv, &p95Latency); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		results = append(results, map[string]interface{}{
			"minute":        minute,
			"txn_count":     txnCount,
			"success_rate":  successRate,
			"gmv":           gmv,
			"p95_latency":   p95Latency,
		})
	}

	return results, nil
}

// GetBaselineForZScore retrieves baseline metrics from 7 days ago for Z-score calculation
func (c *Client) GetBaselineForZScore(ctx context.Context, metric, merchantID, gateway string) (float64, float64, error) {
	var avgValue, stddevValue float64

	query := `
		SELECT 
			avg(success_rate) AS avg_value,
			stddev(success_rate) AS stddev_value
		FROM payments.payment_aggregates_1m
		WHERE window_start BETWEEN now() - INTERVAL 8 DAY AND now() - INTERVAL 7 DAY
	`

	args := make([]interface{}, 0)
	if merchantID != "" {
		query += " AND merchant_id = ?"
		args = append(args, merchantID)
	}
	if gateway != "" {
		query += " AND gateway = ?"
		args = append(args, gateway)
	}

	err := c.conn.QueryRow(ctx, query, args...).Scan(&avgValue, &stddevValue)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query baseline: %w", err)
	}

	return avgValue, stddevValue, nil
}

// GetRecentAnomalies retrieves recent anomalies for context
func (c *Client) GetRecentAnomalies(ctx context.Context, limit int) ([]models.AnomalyEvent, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT 
			id, type, severity, metric, merchant_id, gateway, method,
			current_value, baseline_value, deviation, detected_at,
			duration_seconds, acknowledged, resolved
		FROM payments.anomaly_events
		WHERE detected_at >= now() - INTERVAL 30 DAY
		ORDER BY detected_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query anomalies: %w", err)
	}
	defer rows.Close()

	var anomalies []models.AnomalyEvent
	for rows.Next() {
		var a models.AnomalyEvent
		var durationSeconds uint32

		if err := rows.Scan(
			&a.ID, &a.Type, &a.Severity, &a.Metric, &a.MerchantID, &a.Gateway, &a.Method,
			&a.CurrentValue, &a.BaselineValue, &a.Deviation, &a.DetectedAt,
			&durationSeconds, &a.Acknowledged, &a.Resolved,
		); err != nil {
			return nil, fmt.Errorf("failed to scan anomaly: %w", err)
		}

		a.Duration = time.Duration(durationSeconds) * time.Second
		anomalies = append(anomalies, a)
	}

	return anomalies, nil
}

// GetCurrentSuccessRate returns the current overall success rate
func (c *Client) GetCurrentSuccessRate(ctx context.Context, minutes int) (float64, error) {
	var successRate float64

	err := c.conn.QueryRow(ctx, `
		SELECT countIf(status = 'success') * 100.0 / count(*)
		FROM payments.payment_events
		WHERE event_time >= now() - INTERVAL ? MINUTE
	`, minutes).Scan(&successRate)

	if err != nil {
		return 0, fmt.Errorf("failed to query success rate: %w", err)
	}

	return successRate, nil
}

// Close closes the ClickHouse connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
