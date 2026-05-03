package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"payment-monitoring/internal/config"
	"payment-monitoring/pkg/models"
)

// Client handles LLM API interactions
type Client struct {
	config  *config.LLMConfig
	logger  *zap.Logger
	httpClient *http.Client
}

// NewClient creates a new LLM client
func NewClient(cfg *config.LLMConfig, logger *zap.Logger) *Client {
	return &Client{
		config: cfg,
		logger: logger,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// IncidentContext holds all context for incident analysis
type IncidentContext struct {
	Anomaly           models.AnomalyEvent  `json:"anomaly"`
	CurrentMetrics    map[string]float64   `json:"current_metrics"`
	BaselineMetrics   map[string]float64   `json:"baseline_metrics"`
	ErrorDistribution []ErrorDistEntry     `json:"error_distribution"`
	SimilarIncidents  []models.AnomalyEvent `json:"similar_incidents"`
	TimeWindow        string               `json:"time_window"`
	AffectedMerchants []string             `json:"affected_merchants"`
	AffectedGateways  []string             `json:"affected_gateways"`
}

type ErrorDistEntry struct {
	ErrorCode string `json:"error_code"`
	Count     int    `json:"count"`
	Gateway   string `json:"gateway"`
}

// AnalysisRequest is the structured request to LLM
type AnalysisRequest struct {
	Context     string `json:"context"`
	AnomalyDesc string `json:"anomaly_description"`
}

// AnalysisResponse is the parsed response from LLM
type AnalysisResponse struct {
	RootCauses       []string `json:"root_causes"`
	ImmediateActions []string `json:"immediate_actions"`
	ImpactEstimate   string   `json:"impact_estimate"`
	SimilarIncidents []string `json:"similar_incidents"`
	EscalationLevel  string   `json:"escalation_level"`
	ConfidenceScore  float32  `json:"confidence_score"`
}

// AnalyzeIncident sends incident context to LLM and gets analysis
func (c *Client) AnalyzeIncident(ctx context.Context, incidentCtx IncidentContext) (*models.IncidentAnalysis, error) {
	// Build prompt
	prompt := c.buildPrompt(incidentCtx)

	c.logger.Info("analyzing incident with LLM",
		zap.String("severity", incidentCtx.Anomaly.Severity),
		zap.String("metric", incidentCtx.Anomaly.Metric))

	var response AnalysisResponse
	var err error

	switch c.config.Provider {
	case "claude":
		response, err = c.callClaude(ctx, prompt)
	case "openai":
		response, err = c.callOpenAI(ctx, prompt)
	default:
		// Default to Claude
		response, err = c.callClaude(ctx, prompt)
	}

	if err != nil {
		return nil, fmt.Errorf("LLM analysis failed: %w", err)
	}

	analysis := &models.IncidentAnalysis{
		RootCauses:       response.RootCauses,
		ImmediateActions: response.ImmediateActions,
		ImpactEstimate:   response.ImpactEstimate,
		SimilarIncidents: response.SimilarIncidents,
		EscalationLevel:  response.EscalationLevel,
		ConfidenceScore:  response.ConfidenceScore,
		GeneratedAt:      time.Now(),
	}

	return analysis, nil
}

func (c *Client) buildPrompt(incidentCtx IncidentContext) string {
	contextJSON, _ := json.MarshalIndent(incidentCtx, "", "  ")

	anomalyDesc := fmt.Sprintf(
		"Detected anomaly: %s on metric %s. Current value: %.2f, Baseline: %.2f, Deviation: %.2f%%. Severity: %s. Affected gateway: %s, merchant: %s",
		incidentCtx.Anomaly.Type,
		incidentCtx.Anomaly.Metric,
		incidentCtx.Anomaly.CurrentValue,
		incidentCtx.Anomaly.BaselineValue,
		incidentCtx.Anomaly.Deviation,
		incidentCtx.Anomaly.Severity,
		incidentCtx.Anomaly.Gateway,
		incidentCtx.Anomaly.MerchantID,
	)

	prompt := fmt.Sprintf(`You are an expert payments SRE analyzing a production incident. Your analysis will be used by on-call engineers to quickly resolve the issue.

## Context
%s

## Detected Anomaly
%s

## Instructions
Provide a concise, actionable analysis with the following sections:

1. **ROOT CAUSES** (ranked by probability, 3-5 bullet points)
   - Be specific about what could cause THIS pattern
   - Reference actual numbers from the context
   
2. **IMMEDIATE ACTIONS** (prioritized checklist)
   - First: What to check/investigate
   - Second: What to mitigate
   - Third: What to fix
   
3. **IMPACT ESTIMATE**
   - Estimate affected transactions/revenue
   - Customer impact assessment
   
4. **SIMILAR PAST INCIDENTS**
   - Pattern match against similar anomalies
   - What resolved them before
   
5. **ESCALATION RECOMMENDATION**
   - P0: Critical - all hands, exec notification
   - P1: High - engineering team mobilization  
   - P2: Medium - standard on-call response
   - P3: Low - monitor and document

Format your response as valid JSON matching this schema:
{
  "root_causes": ["cause 1", "cause 2", ...],
  "immediate_actions": ["action 1", "action 2", ...],
  "impact_estimate": "description of impact",
  "similar_incidents": ["incident pattern 1", ...],
  "escalation_level": "P0|P1|P2|P3",
  "confidence_score": 0.0-1.0
}

Be specific. Reference the actual metrics. Think step by step.`,
		string(contextJSON), anomalyDesc)

	return prompt
}

func (c *Client) callClaude(ctx context.Context, prompt string) (AnalysisResponse, error) {
	requestBody := map[string]interface{}{
		"model": c.config.Model,
		"max_tokens": c.config.MaxTokens,
		"temperature": c.config.Temperature,
		"messages": []map[string]string{
			{
				"role": "user",
				"content": prompt,
			},
		},
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return AnalysisResponse{}, err
	}

	url := "https://api.anthropic.com/v1/messages"
	if c.config.EndpointURL != "" {
		url = c.config.EndpointURL
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return AnalysisResponse{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", c.config.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return AnalysisResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return AnalysisResponse{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return AnalysisResponse{}, fmt.Errorf("API error: %s", string(body))
	}

	// Parse Claude response
	var claudeResp struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}

	if err := json.Unmarshal(body, &claudeResp); err != nil {
		return AnalysisResponse{}, err
	}

	if len(claudeResp.Content) == 0 {
		return AnalysisResponse{}, fmt.Errorf("empty response from Claude")
	}

	// Extract JSON from response
	content := claudeResp.Content[0].Text
	return c.parseAnalysisResponse(content)
}

func (c *Client) callOpenAI(ctx context.Context, prompt string) (AnalysisResponse, error) {
	requestBody := map[string]interface{}{
		"model": c.config.Model,
		"max_tokens": c.config.MaxTokens,
		"temperature": c.config.Temperature,
		"messages": []map[string]string{
			{
				"role": "system",
				"content": "You are an expert payments SRE. Respond with valid JSON only.",
			},
			{
				"role": "user",
				"content": prompt,
			},
		},
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return AnalysisResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", 
		"https://api.openai.com/v1/chat/completions", 
		bytes.NewBuffer(jsonData))
	if err != nil {
		return AnalysisResponse{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return AnalysisResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return AnalysisResponse{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return AnalysisResponse{}, fmt.Errorf("API error: %s", string(body))
	}

	// Parse OpenAI response
	var openaiResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(body, &openaiResp); err != nil {
		return AnalysisResponse{}, err
	}

	if len(openaiResp.Choices) == 0 {
		return AnalysisResponse{}, fmt.Errorf("empty response from OpenAI")
	}

	content := openaiResp.Choices[0].Message.Content
	return c.parseAnalysisResponse(content)
}

func (c *Client) parseAnalysisResponse(content string) (AnalysisResponse, error) {
	// Try to extract JSON from the content
	startIdx := strings.Index(content, "{")
	endIdx := strings.LastIndex(content, "}")
	
	if startIdx == -1 || endIdx == -1 {
		return AnalysisResponse{}, fmt.Errorf("no JSON found in response")
	}

	jsonStr := content[startIdx : endIdx+1]

	var response AnalysisResponse
	if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
		// Try to be more lenient - sometimes LLMs add markdown
		jsonStr = strings.TrimPrefix(jsonStr, "```json")
		jsonStr = strings.TrimSuffix(jsonStr, "```")
		if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
			return AnalysisResponse{}, fmt.Errorf("failed to parse JSON response: %w", err)
		}
	}

	return response, nil
}

// GenerateSlackMessage formats the analysis as a Slack Block Kit message
func (c *Client) GenerateSlackMessage(analysis *models.IncidentAnalysis, anomaly *models.AnomalyEvent) map[string]interface{} {
	blocks := []map[string]interface{}{
		{
			"type": "header",
			"text": map[string]interface{}{
				"type":  "plain_text",
				"text":  fmt.Sprintf("🚨 %s Incident Detected", analysis.EscalationLevel),
				"emoji": true,
			},
		},
		{
			"type": "section",
			"fields": []map[string]interface{}{
				{"type": "mrkdwn", "text": fmt.Sprintf("*Metric:* %s", anomaly.Metric)},
				{"type": "mrkdwn", "text": fmt.Sprintf("*Severity:* %s", anomaly.Severity)},
				{"type": "mrkdwn", "text": fmt.Sprintf("*Gateway:* %s", anomaly.Gateway)},
				{"type": "mrkdwn", "text": fmt.Sprintf("*Deviation:* %.1f%%", anomaly.Deviation)},
			},
		},
		{
			"type": "divider",
		},
		{
			"type": "section",
			"text": map[string]interface{}{
				"type": "mrkdwn",
				"text": "*🔍 Likely Root Causes*",
			},
		},
	}

	for i, cause := range analysis.RootCauses {
		blocks = append(blocks, map[string]interface{}{
			"type": "section",
			"text": map[string]interface{}{
				"type": "mrkdwn",
				"text": fmt.Sprintf("%d. %s", i+1, cause),
			},
		})
	}

	blocks = append(blocks, map[string]interface{}{
		"type": "divider",
	}, map[string]interface{}{
		"type": "section",
		"text": map[string]interface{}{
			"type": "mrkdwn",
			"text": "*✅ Immediate Actions*",
		},
	})

	for i, action := range analysis.ImmediateActions {
		blocks = append(blocks, map[string]interface{}{
			"type": "section",
			"text": map[string]interface{}{
				"type": "mrkdwn",
				"text": fmt.Sprintf("%d. %s", i+1, action),
			},
		})
	}

	blocks = append(blocks, map[string]interface{}{
		"type": "divider",
	}, map[string]interface{}{
		"type": "section",
		"text": map[string]interface{}{
			"type": "mrkdwn",
			"text": fmt.Sprintf("*💰 Impact Estimate:* %s\n*🎯 Confidence:* %.0f%%", 
				analysis.ImpactEstimate, analysis.ConfidenceScore*100),
		},
	})

	// Action buttons
	blocks = append(blocks, map[string]interface{}{
		"type": "actions",
		"elements": []map[string]interface{}{
			{
				"type": "button",
				"text": map[string]interface{}{"type": "plain_text", "text": "✓ Acknowledge"},
				"value": "acknowledge",
				"action_id": "ack_incident",
			},
			{
				"type": "button",
				"text": map[string]interface{}{"type": "plain_text", "text": "⬆️ Escalate"},
				"value": "escalate",
				"action_id": "escalate_incident",
			},
			{
				"type": "button",
				"text": map[string]interface{}{"type": "plain_text", "text": "📊 Dashboard"},
				"url": "http://localhost:3000/d/anomaly-center",
				"action_id": "view_dashboard",
			},
		},
	})

	return map[string]interface{}{
		"blocks": blocks,
	}
}
