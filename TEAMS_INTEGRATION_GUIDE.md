# Microsoft Teams Integration Guide

This guide covers setting up Microsoft Teams notifications for the Data Streaming Framework's alert system, providing real-time notifications for critical system events.

## 🚀 Overview

The Teams integration sends structured alert cards to Microsoft Teams channels, providing:

- **Rich formatted alerts** with adaptive cards
- **Context-aware information** including error details and system metrics
- **Actionable buttons** for quick access to logs and dashboards
- **Color-coded severity levels** for immediate visual recognition
- **Automatic error classification** and priority handling

## 📋 Setup Instructions

### 1. Create Teams Webhook

**Step 1: Open Microsoft Teams**
- Navigate to the channel where you want to receive alerts
- Click on the **three dots (...)** next to the channel name
- Select **Connectors**

**Step 2: Add Incoming Webhook**
- Search for "Incoming Webhook"
- Click **Add** next to "Incoming Webhook"
- Click **Add** again to configure

**Step 3: Configure Webhook**
- **Name**: "Data Streaming Framework Alerts"
- **Description**: "Critical and high-priority alerts from the streaming platform"
- Upload a custom icon (optional)
- Click **Create**

**Step 4: Copy Webhook URL**
- Copy the webhook URL (starts with `https://outlook.office.com/webhook/` or `https://outlook.office365.com/webhook/`)
- Store this securely - you'll need it for configuration

### 2. Framework Configuration

**Basic Configuration:**
```yaml
datastreaming:
  alerts:
    enabled: true
    notifiers:
      teams:
        enabled: true
        webhookUrl: "${TEAMS_WEBHOOK_URL}"
        title: "Data Streaming Framework Alert"
        minLevel: "HIGH"  # Send HIGH and CRITICAL alerts to Teams
```

**Environment Variables:**
```bash
# Required
export TEAMS_WEBHOOK_URL="https://outlook.office.com/webhook/your-webhook-url"

# Optional customization
export TEAMS_CARD_TITLE="Production Data Streaming Alert"
export TEAMS_MIN_LEVEL="HIGH"
export TEAMS_ALERTS_ENABLED="true"
```

**Production Configuration (application-prod.yml):**
```yaml
datastreaming:
  alerts:
    notifiers:
      teams:
        enabled: true
        webhookUrl: "${TEAMS_PROD_WEBHOOK}"
        title: "PRODUCTION - Data Streaming Alert"
        minLevel: "HIGH"
```

**Development Configuration (application-dev.yml):**
```yaml
datastreaming:
  alerts:
    notifiers:
      teams:
        enabled: true
        webhookUrl: "${TEAMS_DEV_WEBHOOK}"
        title: "DEV Environment Alert"
        minLevel: "WARNING"  # More verbose in development
```

## 🎨 Alert Card Features

### Visual Elements

**Color Coding:**
- 🔴 **CRITICAL**: Red theme - Immediate attention required
- 🟠 **HIGH**: Orange theme - High priority issues
- 🟡 **WARNING**: Gold theme - Potential issues
- 🔵 **INFO**: Blue theme - Informational messages

**Card Structure:**
```
┌─────────────────────────────────────────┐
│  🔴 Data Streaming Framework Alert      │
│  CRITICAL Alert from MQConsumerWorker   │
├─────────────────────────────────────────┤
│  **Authentication Failed**             │
│                                         │
│  Unable to authenticate with MQ server │
│  Connection refused by PROD_QM_01       │
├─────────────────────────────────────────┤
│  Alert Level:    CRITICAL               │
│  Source:         MQConsumerWorker       │
│  Instance:       streaming-app-01       │
│  Timestamp:      2024-01-15 14:30:25    │
│  Worker Id:      worker-abc123          │
│  Queue Name:     TRANSACTION.QUEUE      │
│  Error Type:     JMSSecurityException   │
├─────────────────────────────────────────┤
│  [View Logs] [Dashboard] [Health Check] │
└─────────────────────────────────────────┘
```

### Context Information

The Teams notifier automatically includes:

**System Information:**
- Alert level and source component
- Instance ID and timestamp
- Unique alert ID for tracking

**Error Details:**
- Exception type and message
- Relevant context from the alert
- Component-specific metrics

**Actionable Links:**
- Direct links to log management system
- Monitoring dashboard access
- Health check endpoints

## 🔧 Customization Options

### Alert Filtering

**By Level:**
```yaml
datastreaming:
  alerts:
    notifiers:
      teams:
        minLevel: "CRITICAL"  # Only critical alerts
```

**By Source:**
```yaml
datastreaming:
  alerts:
    notifiers:
      teams:
        enabled: true
        sourceFilters:
          - "MQConsumerWorker"
          - "KafkaProducer"
          # Only alerts from these sources
```

### Custom Card Title

**Environment-Specific Titles:**
```yaml
# Production
teams:
  title: "🚨 PRODUCTION ALERT - Data Streaming"

# Staging  
teams:
  title: "⚠️ STAGING ALERT - Data Streaming"

# Development
teams:
  title: "🔧 DEV ALERT - Data Streaming"
```

### Action Button Configuration

Customize the action buttons in critical alerts:

```java
// In your configuration
@Value("${monitoring.dashboard.url:https://grafana.company.com}")
private String dashboardUrl;

@Value("${logging.system.url:https://kibana.company.com}")
private String loggingUrl;
```

## 📊 Alert Examples

### Critical Alert Example
```json
{
  "@type": "MessageCard",
  "@context": "https://schema.org/extensions",
  "summary": "Critical MQ Connection Failure",
  "themeColor": "FF0000",
  "sections": [
    {
      "activityTitle": "Data Streaming Framework Alert",
      "activitySubtitle": "**CRITICAL Alert from MQConsumerWorker**",
      "text": "**MQ Connection Failed**\n\nAuthentication failed for queue manager PROD_QM_01",
      "markdown": true
    },
    {
      "facts": [
        {"name": "Alert Level", "value": "CRITICAL"},
        {"name": "Source", "value": "MQConsumerWorker"},
        {"name": "Instance", "value": "streaming-app-01"},
        {"name": "Worker Id", "value": "worker-abc123"},
        {"name": "Queue Name", "value": "TRANSACTION.QUEUE"},
        {"name": "Error Type", "value": "JMSSecurityException"}
      ]
    }
  ],
  "potentialAction": [
    {
      "@type": "OpenUri",
      "name": "View Logs",
      "targets": [{"os": "default", "uri": "https://logs.company.com/search?q=alert-id"}]
    },
    {
      "@type": "OpenUri", 
      "name": "View Dashboard",
      "targets": [{"os": "default", "uri": "https://grafana.company.com/datastreaming"}]
    }
  ]
}
```

### High Priority Alert Example
```
🟠 Data Streaming Framework Alert
HIGH Alert from KafkaProducer

**High Error Rate Detected**
Error rate exceeded 5% threshold: 8.3%

Alert Level:     HIGH
Source:          KafkaProducer  
Instance:        streaming-app-02
Error Rate:      8.3%
Topic:           financial-transactions
Total Messages:  1,247
Failed Messages: 103
```

## 🔍 Troubleshooting

### Common Issues

**1. Webhook URL Not Working**
```
Error: HTTP 400 Bad Request from Teams webhook
```
**Solution:** 
- Verify webhook URL is correct and active
- Check that the Teams connector is still configured
- Ensure webhook URL starts with `https://outlook.office.com/webhook/`

**2. Cards Not Displaying Properly**
```
Teams shows plain text instead of formatted card
```
**Solution:**
- Verify JSON structure matches Adaptive Card schema
- Check that `@type` and `@context` are correctly set
- Ensure `markdown: true` is set for formatted text

**3. No Alerts Received**
```
Teams notifier shows as healthy but no alerts appear
```
**Solution:**
- Check alert level configuration (`minLevel`)
- Verify Teams notifier is enabled in configuration
- Check application logs for Teams API errors

### Debugging Steps

**1. Test Webhook Manually:**
```bash
curl -X POST "${TEAMS_WEBHOOK_URL}" \
  -H "Content-Type: application/json" \
  -d '{
    "@type": "MessageCard",
    "@context": "https://schema.org/extensions",
    "summary": "Test Alert",
    "text": "This is a test message from the Data Streaming Framework"
  }'
```

**2. Enable Debug Logging:**
```yaml
logging:
  level:
    com.datastreaming.framework.core.alert.TeamsAlertNotifier: DEBUG
```

**3. Check Health Endpoint:**
```bash
curl http://localhost:8080/actuator/health/alertingHealth
```

### Health Check

The Teams notifier includes health validation:

```java
@Override
public boolean isHealthy() {
    // Validates webhook URL format
    return webhookUrl != null && 
           (webhookUrl.startsWith("https://outlook.office.com/webhook/") ||
            webhookUrl.startsWith("https://outlook.office365.com/webhook/"));
}
```

## 🛡️ Security Considerations

### Webhook URL Security
- **Store webhook URLs securely** - treat them as secrets
- **Use environment variables** instead of hardcoding URLs
- **Rotate webhook URLs** periodically for security
- **Limit webhook permissions** in Teams channel settings

### Information Disclosure
- **Sensitive data filtering** - framework automatically filters sensitive information
- **Error message truncation** - long error messages are truncated to prevent information leakage
- **Context sanitization** - alert context is sanitized before sending

### Network Security
```yaml
datastreaming:
  alerts:
    notifiers:
      teams:
        # Optional: Configure proxy if needed
        proxyHost: "proxy.company.com"
        proxyPort: 8080
        # Optional: Timeout settings
        connectionTimeoutMs: 5000
        readTimeoutMs: 10000
```

## 📝 Best Practices

### Channel Management
- **Separate channels** for different environments (prod, staging, dev)
- **Dedicated channels** for different alert levels
- **Channel naming** should clearly indicate purpose: `#datastreaming-prod-alerts`

### Alert Optimization
- **Use appropriate levels** - don't spam with too many INFO alerts
- **Include actionable information** - ensure alerts provide next steps
- **Context is key** - include relevant system state information
- **Monitor alert volume** - use framework metrics to track alert frequency

### Team Integration
- **@mention relevant teams** for critical alerts using Teams webhook features
- **Set up notification schedules** for different teams
- **Create alert response procedures** and document them in Teams wiki

## 🔗 Integration with Other Systems

### Log Management
```yaml
# Configure log system URLs for alert actions
datastreaming:
  monitoring:
    logSystemUrl: "https://kibana.company.com"
    logSearchPattern: "/app/discover#/?_g=(filters:!(),query:(match:(alert_id:(query:'{{ALERT_ID}}',type:phrase))))"
```

### Monitoring Dashboard
```yaml
# Configure dashboard URLs for alert actions  
datastreaming:
  monitoring:
    dashboardUrl: "https://grafana.company.com"
    dashboardPath: "/d/datastreaming/data-streaming-overview?var-instance={{INSTANCE_ID}}"
```

### Incident Management
```yaml
# Optional: Integration with incident management
datastreaming:
  alerts:
    incidents:
      enabled: true
      createForLevels: ["CRITICAL"]
      servicenowUrl: "https://company.service-now.com"
```

## 📈 Monitoring Teams Notifications

### Metrics Available
```prometheus
# Teams notification metrics
datastreaming_teams_notifications_sent_total{level="CRITICAL"} 5
datastreaming_teams_notifications_failed_total{level="HIGH"} 1
datastreaming_teams_notification_latency_ms{quantile="0.95"} 250
```

### Health Monitoring
```bash
# Check Teams notifier health
curl http://localhost:8080/actuator/health | jq '.components.alertingHealth.details.healthyNotifiers'
```

This comprehensive Teams integration provides enterprise-grade alerting with rich formatting, actionable information, and robust error handling for your Data Streaming Framework.