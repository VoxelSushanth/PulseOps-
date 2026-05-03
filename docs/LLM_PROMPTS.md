# LLM Prompt Templates for Incident Analysis

## System Prompt (Claude)

```
You are an expert payments Site Reliability Engineer (SRE) with deep knowledge of payment processing systems, gateway integrations, and incident response. Your analysis will be used by on-call engineers to quickly diagnose and resolve production incidents.

When analyzing incidents:
1. Be specific and reference actual numbers from the provided context
2. Prioritize root causes by likelihood based on the symptoms
3. Provide actionable, prioritized steps
4. Consider both technical and operational factors
5. Estimate business impact realistically
```

## User Prompt Template

```markdown
## Context
{JSON_CONTEXT}

## Detected Anomaly
{ANOMALY_DESCRIPTION}

## Instructions
Provide a concise, actionable analysis with the following sections:

### 1. ROOT CAUSES (ranked by probability, 3-5 bullet points)
- Be specific about what could cause THIS pattern
- Reference actual numbers from the context
- Consider: gateway issues, network problems, capacity constraints, recent deployments, external dependencies

### 2. IMMEDIATE ACTIONS (prioritized checklist)
- First: What to check/investigate in the next 5 minutes
- Second: What to mitigate in the next 15 minutes  
- Third: What to fix in the next hour

### 3. IMPACT ESTIMATE
- Estimate affected transactions based on traffic patterns
- Calculate potential revenue impact
- Assess customer experience impact

### 4. SIMILAR PAST INCIDENTS
- Pattern match against known incident types
- What resolved them before
- Any recurring themes

### 5. ESCALATION RECOMMENDATION
- P0: Critical - all hands, exec notification, war room
- P1: High - engineering team mobilization, stakeholder updates
- P2: Medium - standard on-call response, monitor closely
- P3: Low - document and track, no immediate action needed

Format your response as valid JSON matching this schema:
{
  "root_causes": ["cause 1", "cause 2", ...],
  "immediate_actions": ["action 1", "action 2", ...],
  "impact_estimate": "description of impact",
  "similar_incidents": ["incident pattern 1", ...],
  "escalation_level": "P0|P1|P2|P3",
  "confidence_score": 0.0-1.0
}

Be specific. Reference the actual metrics. Think step by step.
```

## Example Context JSON

```json
{
  "anomaly": {
    "type": "ewma,cusum",
    "severity": "P1",
    "metric": "success_rate",
    "merchant_id": "",
    "gateway": "HDFC",
    "current_value": 87.5,
    "baseline_value": 98.2,
    "deviation": -10.9
  },
  "current_metrics": {
    "success_rate": 87.5,
    "txn_count_5m": 12500,
    "p95_latency_ms": 450,
    "timeout_rate": 8.2
  },
  "baseline_metrics": {
    "success_rate": 98.2,
    "txn_count_5m": 12800,
    "p95_latency_ms": 180,
    "timeout_rate": 0.5
  },
  "error_distribution": [
    {"error_code": "GATEWAY_TIMEOUT", "count": 890, "gateway": "HDFC"},
    {"error_code": "CONNECTION_REFUSED", "count": 145, "gateway": "HDFC"},
    {"error_code": "INVALID_RESPONSE", "count": 67, "gateway": "HDFC"}
  ],
  "similar_incidents": [
    {
      "date": "2024-01-15",
      "description": "HDFC gateway network partition",
      "resolution": "Failover to secondary endpoint"
    }
  ],
  "time_window": "last_5_minutes",
  "affected_merchants": ["merchant_001", "merchant_003", "merchant_007"],
  "affected_gateways": ["HDFC"]
}
```

## Expected Response Format

```json
{
  "root_causes": [
    "HDFC gateway experiencing network connectivity issues - 890 GATEWAY_TIMEOUT errors in 5 minutes vs baseline of ~50",
    "Possible datacenter network partition affecting HDFC primary endpoint - CONNECTION_REFUSED errors increasing",
    "No evidence of application-side issues - other gateways operating normally at 98%+ success rate"
  ],
  "immediate_actions": [
    "Check HDFC gateway status page and contact their NOC",
    "Verify network connectivity to HDFC endpoints from our infrastructure",
    "Consider enabling traffic shift to backup gateway if available",
    "Review recent network changes or maintenance windows"
  ],
  "impact_estimate": "Approximately 1,300 failed transactions in last 5 minutes. At $50 average transaction value, ~$65K revenue at risk. Affecting ~15% of total platform volume.",
  "similar_incidents": [
    "Jan 15, 2024: HDFC network partition - resolved by failover to secondary endpoint",
    "Nov 3, 2023: HDFC capacity issue during flash sale - resolved by rate limiting"
  ],
  "escalation_level": "P1",
  "confidence_score": 0.85
}
```

## Slack Message Generation

After receiving LLM analysis, format as Slack Block Kit:

```json
{
  "blocks": [
    {
      "type": "header",
      "text": {
        "type": "plain_text",
        "text": "🚨 P1 Incident Detected",
        "emoji": true
      }
    },
    {
      "type": "section",
      "fields": [
        {"type": "mrkdwn", "text": "*Metric:* success_rate"},
        {"type": "mrkdwn", "text": "*Severity:* P1"},
        {"type": "mrkdwn", "text": "*Gateway:* HDFC"},
        {"type": "mrkdwn", "text": "*Deviation:* -10.9%"}
      ]
    },
    {
      "type": "divider"
    },
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*🔍 Likely Root Causes*"
      }
    },
    ...
  ]
}
```

## Tips for Better LLM Analysis

1. **Include rich context**: More metrics = better analysis
2. **Provide historical comparison**: Same time yesterday/last week
3. **Include error code distribution**: Helps identify failure patterns
4. **List similar past incidents**: LLM can pattern match
5. **Specify affected scope**: Gateway-specific vs platform-wide
6. **Add recent changes**: Deployments, config changes, etc.
