package anomaly

import (
	"math"
	"sync"
	"time"

	"payment-monitoring/internal/config"
	"payment-monitoring/pkg/models"
)

// EWMA implements Exponentially Weighted Moving Average
type EWMA struct {
	alpha   float64
	value   float64
	init    bool
	mu      sync.RWMutex
}

// NewEWMA creates a new EWMA detector
func NewEWMA(alpha float64) *EWMA {
	return &EWMA{
		alpha: alpha,
	}
}

// Update updates the EWMA with a new value and returns the deviation
func (e *EWMA) Update(value float64) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.init {
		e.value = value
		e.init = true
		return 0
	}

	prev := e.value
	e.value = e.alpha*value + (1-e.alpha)*e.value
	
	deviation := value - prev
	return deviation
}

// GetValue returns current EWMA value
func (e *EWMA) GetValue() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.value
}

// IsAnomaly checks if the current value deviates significantly from EWMA
func (e *EWMA) IsAnomaly(value float64, threshold float64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.init {
		return false
	}

	deviation := math.Abs(value - e.value)
	expectedRange := math.Abs(e.value) * threshold
	
	return deviation > expectedRange
}

// CUSUM implements Cumulative Sum control chart for detecting sustained drift
type CUSUM struct {
	target    float64
	cusumPos  float64
	cusumNeg  float64
	threshold float64
	slack     float64
	mu        sync.RWMutex
}

// NewCUSUM creates a new CUSUM detector
func NewCUSUM(target, threshold, slack float64) *CUSUM {
	return &CUSUM{
		target:    target,
		threshold: threshold,
		slack:     slack,
	}
}

// Update updates CUSUM with a new value and returns whether drift is detected
func (c *CUSUM) Update(value float64) (driftUp, driftDown bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	diff := value - c.target

	// Update positive CUSUM (detecting upward drift)
	c.cusumPos += diff - c.slack
	if c.cusumPos < 0 {
		c.cusumPos = 0
	}

	// Update negative CUSUM (detecting downward drift)
	c.cusumNeg += -diff - c.slack
	if c.cusumNeg < 0 {
		c.cusumNeg = 0
	}

	driftUp = c.cusumPos > c.threshold
	driftDown = c.cusumNeg > c.threshold

	return driftUp, driftDown
}

// GetValues returns current CUSUM values
func (c *CUSUM) GetValues() (float64, float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cusumPos, c.cusumNeg
}

// Reset resets CUSUM state
func (c *CUSUM) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cusumPos = 0
	c.cusumNeg = 0
}

// ZScoreDetector implements Z-score based anomaly detection
type ZScoreDetector struct {
	mean      float64
	stddev    float64
	threshold float64
	count     int
	mu        sync.RWMutex
}

// NewZScoreDetector creates a new Z-score detector
func NewZScoreDetector(threshold float64) *ZScoreDetector {
	return &ZScoreDetector{
		threshold: threshold,
	}
}

// SetBaseline sets the baseline mean and stddev
func (z *ZScoreDetector) SetBaseline(mean, stddev float64) {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.mean = mean
	z.stddev = stddev
}

// IsAnomaly checks if value is anomalous based on Z-score
func (z *ZScoreDetector) IsAnomaly(value float64) (bool, float64) {
	z.mu.RLock()
	defer z.mu.RUnlock()

	if z.stddev == 0 {
		return false, 0
	}

	zscore := math.Abs((value - z.mean) / z.stddev)
	return zscore > z.threshold, zscore
}

// Detector combines multiple anomaly detection methods
type Detector struct {
	config          *config.ProcessorConfig
	ewmaDetectors   map[string]*EWMA
	cusumDetectors  map[string]*CUSUM
	zscoreDetectors map[string]*ZScoreDetector
	anomalyHistory  map[string][]time.Time
	mu              sync.RWMutex
}

// NewDetector creates a new combined anomaly detector
func NewDetector(cfg *config.ProcessorConfig) *Detector {
	return &Detector{
		config:          cfg,
		ewmaDetectors:   make(map[string]*EWMA),
		cusumDetectors:  make(map[string]*CUSUM),
		zscoreDetectors: make(map[string]*ZScoreDetector),
		anomalyHistory:  make(map[string][]time.Time),
	}
}

// getOrCreateEWMA gets or creates an EWMA detector for a metric key
func (d *Detector) getOrCreateEWMA(key string) *EWMA {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.ewmaDetectors[key]; !ok {
		d.ewmaDetectors[key] = NewEWMA(d.config.EWMASmoothingFactor)
	}
	return d.ewmaDetectors[key]
}

// getOrCreateCUSUM gets or creates a CUSUM detector for a metric key
func (d *Detector) getOrCreateCUSUM(key string, target float64) *CUSUM {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.cusumDetectors[key]; !ok {
		d.cusumDetectors[key] = NewCUSUM(target, d.config.CUSUMThreshold, 0.5)
	}
	return d.cusumDetectors[key]
}

// getOrCreateZScore gets or creates a Z-score detector for a metric key
func (d *Detector) getOrCreateZScore(key string) *ZScoreDetector {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.zscoreDetectors[key]; !ok {
		d.zscoreDetectors[key] = NewZScoreDetector(d.config.ZScoreThreshold)
	}
	return d.zscoreDetectors[key]
}

// CheckAnomaly checks for anomalies using all three methods
// Returns anomaly event if 2 out of 3 methods agree
func (d *Detector) CheckAnomaly(
	metricKey string,
	metricName string,
	currentValue float64,
	baselineMean float64,
	baselineStddev float64,
	merchantID string,
	gateway string,
	method string,
) *models.AnomalyEvent {

	ewma := d.getOrCreateEWMA(metricKey)
	cusum := d.getOrCreateCUSUM(metricKey, baselineMean)
	zscore := d.getOrCreateZScore(metricKey)

	// Update detectors
	ewmaDeviation := ewma.Update(currentValue)
	cusumUp, cusumDown := cusum.Update(currentValue)
	zscore.SetBaseline(baselineMean, baselineStddev)
	isZscoreAnomaly, zscoreValue := zscore.IsAnomaly(currentValue)

	// Count how many methods detect anomaly
	anomalyCount := 0
	anomalyType := ""

	// EWMA check: significant deviation from smoothed value
	ewmaThreshold := 0.3 // 30% deviation
	if math.Abs(ewmaDeviation) > math.Abs(baselineMean)*ewmaThreshold {
		anomalyCount++
		anomalyType = "ewma"
	}

	// CUSUM check: sustained drift
	if cusumUp || cusumDown {
		anomalyCount++
		if anomalyType == "" {
			anomalyType = "cusum"
		} else {
			anomalyType = "ewma,cusum"
		}
	}

	// Z-score check: statistical outlier
	if isZscoreAnomaly {
		anomalyCount++
		if anomalyType == "" {
			anomalyType = "zscore"
		} else {
			anomalyType += ",zscore"
		}
	}

	// Require at least 2 methods to agree
	if anomalyCount < d.config.AnomalyAgreementCount {
		return nil
	}

	// Determine severity based on deviation magnitude
	severity := d.determineSeverity(metricName, currentValue, baselineMean)

	// Calculate deviation percentage
	deviation := 0.0
	if baselineMean != 0 {
		deviation = (currentValue - baselineMean) / baselineMean * 100
	}

	anomaly := &models.AnomalyEvent{
		ID:            generateAnomalyID(metricKey),
		Type:          anomalyType,
		Severity:      severity,
		Metric:        metricName,
		MerchantID:    merchantID,
		Gateway:       gateway,
		Method:        method,
		CurrentValue:  currentValue,
		BaselineValue: baselineMean,
		Deviation:     deviation,
		DetectedAt:    time.Now(),
		Context: map[string]interface{}{
			"ewma_deviation":  ewmaDeviation,
			"cusum_up":        cusumUp,
			"cusum_down":      cusumDown,
			"zscore":          zscoreValue,
			"baseline_mean":   baselineMean,
			"baseline_stddev": baselineStddev,
		},
	}

	// Record in history
	d.recordAnomaly(metricKey)

	return anomaly
}

// determineSeverity determines alert severity based on metric type and deviation
func (d *Detector) determineSeverity(metricName string, currentValue, baselineMean float64) string {
	switch metricName {
	case "success_rate":
		if currentValue < 90 {
			return "P0"
		} else if currentValue < 95 {
			return "P1"
		}
		return "P2"
	case "p99_latency":
		if currentValue > 5000 {
			return "P0"
		} else if currentValue > 2000 {
			return "P1"
		}
		return "P2"
	case "failure_rate":
		if currentValue > 10 {
			return "P0"
		} else if currentValue > 5 {
			return "P1"
		}
		return "P2"
	default:
		return "P3"
	}
}

// recordAnomaly records anomaly in history for rate limiting
func (d *Detector) recordAnomaly(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if _, ok := d.anomalyHistory[key]; !ok {
		d.anomalyHistory[key] = []time.Time{now}
		return
	}

	// Keep only last 30 minutes of history
	var recent []time.Time
	for _, t := range d.anomalyHistory[key] {
		if now.Sub(t) < 30*time.Minute {
			recent = append(recent, t)
		}
	}
	d.anomalyHistory[key] = append(recent, now)
}

// ShouldSuppress checks if an anomaly should be suppressed due to recent alerts
func (d *Detector) ShouldSuppress(key string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	history, ok := d.anomalyHistory[key]
	if !ok {
		return false
	}

	// Suppress if alerted within last 30 minutes
	now := time.Now()
	for _, t := range history {
		if now.Sub(t) < 30*time.Minute {
			return true
		}
	}

	return false
}

func generateAnomalyID(key string) string {
	return fmt.Sprintf("anomaly_%s_%d", key, time.Now().UnixNano())
}

var _ = fmt.Sprint // Import fmt for generateAnomalyID
