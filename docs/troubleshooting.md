# Troubleshooting Guide

This guide covers common issues, diagnostic steps, alert remediation, and frequently asked questions for the Camunda Backup DR tool.

## Table of Contents

- [Common Issues](#common-issues)
  - [Startup Failures](#startup-failures)
  - [Backup Failures](#backup-failures)
  - [Scheduler Issues](#scheduler-issues)
  - [API Errors](#api-errors)
  - [Circuit Breaker Issues](#circuit-breaker-issues)
- [Diagnostic Steps](#diagnostic-steps)
  - [Enable Debug Logging](#enable-debug-logging)
  - [Use Health Endpoints](#use-health-endpoints)
  - [Check Backup Status and History](#check-backup-status-and-history)
  - [Read Backup Logs](#read-backup-logs)
- [Alert Troubleshooting](#alert-troubleshooting)
- [Environment Variable Checklist](#environment-variable-checklist)
- [FAQ](#faq)

---

## Common Issues

### Startup Failures

#### Port Already in Use

**Symptom:** The service exits immediately on startup with a "bind: address already in use" error.

```
INFO  Starting Camunda Backup Controller...
FATAL listen tcp :8080: bind: address already in use
```

**Cause:** Another process is already listening on the configured port.

**Resolution:**
1. Find the process using the port:
   ```bash
   lsof -i :8080
   # or
   ss -tlnp | grep 8080
   ```
2. Stop the conflicting process, or change the port:
   ```bash
   export PORT=9090
   ```

---

#### Invalid Configuration

**Symptom:** The service exits on startup with `Failed to load configuration: invalid configuration`.

```
FATAL Failed to load configuration: invalid configuration
```

**Cause:** One or more environment variables have invalid values. The configuration validator checks:
- `PORT` must be between 1 and 65535.
- `LOG_LEVEL` must be one of: `debug`, `info`, `warn`, `error`.
- `DEFAULT_RETENTION_COUNT`, `DEFAULT_SUCCESS_HISTORY`, and `DEFAULT_FAILURE_HISTORY` must be ≥ 0.
- `DEFAULT_BACKUP_POLL_INTERVAL` and `DEFAULT_BACKUP_MAX_ATTEMPTS` must be > 0.

**Resolution:**
1. Review your environment variables against the [Environment Variable Checklist](#environment-variable-checklist).
2. Unset any incorrectly set variables to fall back to defaults:
   ```bash
   unset PORT         # defaults to 8080
   unset LOG_LEVEL    # defaults to "info"
   ```
3. If you set numeric variables with non-numeric values, the parser silently uses the default. But if you set a numeric value that fails validation (e.g., `PORT=0`), the service will not start.

---

#### Missing or Inaccessible Data Directory

**Symptom:** The service fails to start with a file storage initialization error.

```
INFO  Data directory: /data
ERROR Failed to initialize file storage: ...
FATAL Failed to initialize file storage: ...
```

**Cause:** The directory configured by `DATA_DIR` (default: `/data`) does not exist or the process lacks read/write permissions.

**Resolution:**
1. Create the directory:
   ```bash
   mkdir -p /data
   ```
2. Ensure the process has write access:
   ```bash
   chmod 755 /data
   chown <app-user>:<app-group> /data
   ```
3. Or change the data directory:
   ```bash
   export DATA_DIR=/tmp/camunda-backup
   ```

---

### Backup Failures

#### Component Timeout

**Symptom:** A backup completes with `FAILED` status. Logs show a component timed out during status polling.

```
INFO  [BACKUP_ID: bk-20240115-001] Zeebe backup still running (attempt 120/120)
INFO  [BACKUP_ID: bk-20240115-001] Zeebe backup timeout after polling
INFO  [BACKUP_ID: bk-20240115-001] Backup failed: one or more components failed
```

**Cause:** The component (Zeebe, Operate, Tasklist, Optimize) did not report completion within the polling window. The polling window is `DEFAULT_BACKUP_POLL_INTERVAL × DEFAULT_BACKUP_MAX_ATTEMPTS` (default: 5s × 120 = 10 minutes).

**Resolution:**
1. Check the health of the individual Camunda component. Is it overloaded or unresponsive?
2. Increase the polling window if backups are legitimately slow:
   ```bash
   export DEFAULT_BACKUP_MAX_ATTEMPTS=240    # Double the attempts
   # or
   export DEFAULT_BACKUP_POLL_INTERVAL=10    # Poll less frequently
   ```
3. Check network connectivity between the backup controller and the component's backup endpoint.
4. Review the component's own logs for errors.

---

#### Elasticsearch Snapshot Failure

**Symptom:** The Elasticsearch component reports failure during backup.

```
INFO  [BACKUP_ID: bk-20240115-001] Creating Elasticsearch snapshot: bk-20240115-001 (repository: camunda-backup)
ERROR [BACKUP_ID: bk-20240115-001] Failed to create Elasticsearch snapshot: ...
```

**Common causes and resolutions:**

| Cause | Log Clue | Resolution |
|-------|----------|------------|
| Snapshot repository not registered | `repository_missing_exception` | Register the repository in Elasticsearch: `PUT /_snapshot/camunda-backup` |
| Authentication failure | `401 Unauthorized` or `403 Forbidden` | Verify `ELASTICSEARCH_PASSWORD_<INSTANCE_ID>` is correct |
| Repository not configured | `snapshot repository not configured` | Set `DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY` or `ELASTICSEARCH_SNAPSHOT_REPOSITORY_<ID>` |
| Partial snapshot | `elasticsearch snapshot completed partially` | Check Elasticsearch cluster health — some shards may be unassigned |
| Snapshot already exists | `snapshot_name_already_exists_exception` | A backup with the same ID was already created; investigate duplicate backup triggers |

**Diagnostic steps:**
```bash
# Check cluster health
curl -u user:pass https://elasticsearch:9200/_cluster/health

# List snapshot repositories
curl -u user:pass https://elasticsearch:9200/_snapshot

# Check specific snapshot status
curl -u user:pass https://elasticsearch:9200/_snapshot/camunda-backup/bk-20240115-001/_status
```

---

#### S3 Connectivity Issues

**Symptom:** Backup fails early with an error storing the backup ID or history to S3.

```
INFO  [BACKUP_ID: bk-20240115-001] Backup started at 2024-01-15T10:30:00Z
ERROR [BACKUP_ID: bk-20240115-001] Failed to store backup ID in S3: ...
```

**Cause:** The S3 endpoint is unreachable, or the credentials are invalid.

**Resolution:**
1. Verify S3 credentials:
   ```bash
   echo "Endpoint: $DEFAULT_S3_ENDPOINT"
   echo "Access Key: $DEFAULT_S3_ACCESSKEY"
   echo "Secret Key set: $([ -n "$DEFAULT_S3_SECRETKEY" ] && echo yes || echo no)"
   ```
2. Test S3 connectivity:
   ```bash
   # Using AWS CLI or compatible tool
   aws s3 ls --endpoint-url "$DEFAULT_S3_ENDPOINT" s3://camunda-backups/
   ```
3. For instance-specific secret keys, check the env var name matches the normalized instance ID:
   ```bash
   # For instance "prod-cluster", the variable is:
   echo "S3 key set: $([ -n "$S3_SECRETKEY_PROD_CLUSTER" ] && echo yes || echo no)"
   ```
4. If S3 is not configured, the service falls back to mock storage. This is indicated at startup:
   ```
   INFO  S3 credentials not configured, using mock storage
   ```
   Mock storage is not suitable for production — ensure real S3 credentials are provided.

---

#### Backup Already in Progress

**Symptom:** Triggering a manual backup returns HTTP 409.

```json
{
  "error": "backup_in_progress",
  "message": "A backup is already in progress",
  "code": 409
}
```

**Cause:** The system enforces a global concurrency lock — only one backup (scheduled or manual) can run at a time.

**Resolution:**
1. Wait for the current backup to finish. Check the system status:
   ```bash
   curl http://localhost:8080/api/status
   ```
   The `active_backups` field shows if a backup is running.
2. If the backup appears stuck, see [Stuck Backups](#stuck-backups-detected).

---

### Scheduler Issues

#### Jobs Not Running

**Symptom:** Scheduled backups are not executing at the expected times.

**Diagnostic steps:**
1. Verify the scheduler is running:
   ```bash
   curl http://localhost:8080/readyz
   ```
   Expected response:
   ```json
   {
     "status": "ready",
     "timestamp": "...",
     "checks": {
       "scheduler": "running",
       "camunda_manager": "ok"
     }
   }
   ```
   If `scheduler` is `not_running`, the scheduler failed to start.

2. Check system status for job counts:
   ```bash
   curl http://localhost:8080/api/status
   ```
   Verify `enabled_jobs` > 0. If `enabled_jobs` is 0, all instances may be disabled or no instances are registered.

3. Verify the instance is enabled:
   ```bash
   curl http://localhost:8080/api/camundas/<instance-id>
   ```
   Check that `"enabled": true` and `"schedule"` contains a valid cron expression.

4. Enable debug logging to see scheduler tick output:
   ```bash
   export LOG_LEVEL=debug
   ```
   Look for:
   ```
   DEBUG Skipping job for instance prod-1: another backup is in progress
   DEBUG Skipping scheduled backup for disabled instance: prod-1
   ```

**Common causes:**
- The instance is disabled (`enabled: false`).
- Another backup is already in progress (the global lock blocks new jobs).
- The cron expression is invalid (logged as a warning at registration time).
- The scheduler tick interval (default: 1 minute) means jobs may start up to 1 minute after their scheduled time.

---

#### Stuck Backups Detected

**Symptom:** You receive a **Stuck Backup Detected** alert or see the following in logs:

```
ERROR Stuck backup detected: instance prod-1 (job prod-1) has been running for 2h5m0s (threshold: 2h0m0s)
```

**Cause:** A backup job has been running longer than the configured stuck timeout (`BACKUP_STUCK_TIMEOUT_MINUTES`, default: 120 minutes).

**Important:** Stuck detection is **alerting only** — it does **not** kill or cancel the running backup.

**Resolution:**
1. Check the backup logs for the stuck instance:
   ```bash
   # Find recent backups
   curl http://localhost:8080/api/camundas/prod-1/backups?status=RUNNING

   # Check the backup log
   curl http://localhost:8080/api/camundas/prod-1/backups/<backup-id>/logs
   ```
2. Check the health of the underlying Camunda components. The backup may be waiting on a slow component response.
3. If the backup is genuinely stuck, restart the service to clear the lock. Running jobs will be cancelled during graceful shutdown.
4. To adjust the threshold:
   ```bash
   export BACKUP_STUCK_TIMEOUT_MINUTES=180   # Increase to 3 hours
   # or
   export BACKUP_STUCK_TIMEOUT_MINUTES=0     # Disable stuck detection entirely
   ```
5. Only one stuck alert is sent per episode. The alert is deduplicated — subsequent scheduler ticks do not re-alert for the same stuck job.

---

### API Errors

The API returns structured JSON errors. Below is a reference of common error codes and their meaning.

| HTTP Status | Error Code | Meaning | Common Trigger |
|-------------|-----------|---------|----------------|
| 400 | `validation_error` | Request body or parameters are invalid | Missing required fields, invalid JSON, malformed instance ID |
| 404 | `not_found` | The requested resource does not exist | Wrong instance ID, wrong backup ID |
| 409 | `conflict` / `backup_in_progress` | Resource conflict | Creating a duplicate instance, triggering a backup while one is running |
| 409 | `safety_refusal` | Safety check prevented the operation | Attempting to delete the most recent successful backup |
| 500 | `internal_error` | An unexpected server error occurred | Storage failures, unhandled exceptions |
| 500 | `backup_failed` | The backup operation itself failed | Component errors, S3 failures |
| 500 | `cleanup_failed` | Post-failure cleanup failed | S3 move-to-incomplete errors |
| 502 | `external_call_failed` | An upstream service returned an error | Camunda component API errors |
| 503 | `circuit_open` | Circuit breaker is blocking requests | Too many consecutive failures to an external service |
| 504 | `timeout` | The operation timed out | Backup polling exceeded max attempts |

**Example: Debugging a 500 error**

```bash
# 1. Get the error response
curl -s http://localhost:8080/api/camundas/prod-1/backup -X POST | jq .
# {
#   "error": "backup_failed",
#   "message": "backup failed due to elasticsearch timeout",
#   "code": 500,
#   "component": "elasticsearch",
#   "instance_id": "prod-1"
# }

# 2. Check the backup logs for details
curl http://localhost:8080/api/camundas/prod-1/backups
# Find the latest backup ID, then:
curl http://localhost:8080/api/camundas/prod-1/backups/<backup-id>/logs
```

**Note on generic errors:** If the response contains `"error": "internal_error"` with `"message": "an internal error occurred"`, this means the server encountered an unexpected (non-`AppError`) error. Internal details are hidden to prevent information leakage. Check the server logs for the full error.

---

### Circuit Breaker Issues

#### Service Marked as Open

**Symptom:** API requests return HTTP 503 with `circuit_open` error code, or you receive a **Circuit Breaker Open** alert.

```json
{
  "error": "circuit_open",
  "message": "circuit breaker is open for service: elasticsearch",
  "code": 503
}
```

**Cause:** The circuit breaker for an external service has tripped after 5 consecutive failures (default `MaxFailures`). All requests to that service are now rejected immediately.

**Resolution:**
1. **Investigate the root cause.** The circuit opened because the external service failed 5 times in a row. Check:
   - Is the external service (Elasticsearch, Camunda component) healthy and reachable?
   - Are there network issues or DNS resolution failures?
   - Did credentials expire or change?
2. **Wait for auto-recovery.** After the reset timeout (default: 60 seconds), the circuit transitions to HALF_OPEN and allows a probe request. If the probe succeeds, the circuit closes automatically.
3. **Monitor the transition.** Look for log messages indicating state changes:
   ```
   INFO  Circuit elasticsearch: OPEN → HALF_OPEN
   INFO  Circuit elasticsearch: HALF_OPEN → CLOSED
   ```
4. If the underlying issue is resolved but the circuit hasn't reset yet, a service restart will reset all circuit breakers.

---

#### Half-Open Probe Failures

**Symptom:** The circuit breaker transitions to HALF_OPEN but immediately returns to OPEN.

```
INFO  Circuit elasticsearch: OPEN → HALF_OPEN
INFO  Circuit elasticsearch: HALF_OPEN → OPEN
```

**Cause:** The single probe call allowed in HALF_OPEN failed, indicating the external service is still unhealthy.

**Resolution:**
1. The circuit will remain in OPEN state for another reset timeout period (60s), then probe again.
2. Fix the underlying service issue. The circuit will automatically recover once a probe call succeeds.
3. The circuit allows only 1 probe call in HALF_OPEN (default `HalfOpenMaxCalls`). Additional requests during HALF_OPEN are rejected with: `circuit breaker is open: <service> (half-open probe limit reached)`.

---

## Diagnostic Steps

### Enable Debug Logging

Debug logging provides verbose output for troubleshooting scheduler behavior, HTTP client retries, and internal state transitions.

```bash
export LOG_LEVEL=debug
```

Debug messages include source file and line number:

```
DEBUG 2024/01/15 10:30:45 scheduler.go:239: Skipping job for instance prod-1: another backup is in progress
DEBUG 2024/01/15 10:30:45 httpclient.go:87: Retrying request to https://elasticsearch:9200 (attempt 2/3)
```

Valid log levels in order of verbosity: `debug` > `info` > `warn` > `error`.

---

### Use Health Endpoints

The service exposes three health/status endpoints:

#### `GET /healthz` — Liveness Probe

Returns 200 if the service process is alive. Use this for Kubernetes liveness probes.

```bash
curl http://localhost:8080/healthz
```

```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:45Z",
  "checks": {
    "service": "ok"
  }
}
```

#### `GET /readyz` — Readiness Probe

Returns 200 if the service is ready to handle requests. Checks scheduler state and data store access. Use this for Kubernetes readiness probes.

```bash
curl http://localhost:8080/readyz
```

```json
{
  "status": "ready",
  "timestamp": "2024-01-15T10:30:45Z",
  "checks": {
    "scheduler": "running",
    "camunda_manager": "ok"
  }
}
```

If any check fails, returns 503:

```json
{
  "status": "not_ready",
  "timestamp": "2024-01-15T10:30:45Z",
  "checks": {
    "scheduler": "running",
    "camunda_manager": "error"
  }
}
```

#### `GET /api/status` — System Status

Returns detailed operational status including scheduler, storage, instances, and active backups.

```bash
curl http://localhost:8080/api/status
```

```json
{
  "status": "ok",
  "timestamp": "2024-01-15T10:30:45Z",
  "scheduler": {
    "running": true,
    "jobs_count": 3,
    "enabled_jobs": 2
  },
  "storage": {
    "file_storage_healthy": true,
    "s3_storage_healthy": true
  },
  "camunda_instances": {
    "total": 3,
    "enabled": 2,
    "disabled": 1
  },
  "active_backups": 0
}
```

---

### Check Backup Status and History

#### List All Backups for an Instance

```bash
curl http://localhost:8080/api/camundas/<instance-id>/backups
```

#### Filter by Status

```bash
# Only completed backups
curl http://localhost:8080/api/camundas/<instance-id>/backups?status=COMPLETED

# Only failed backups
curl http://localhost:8080/api/camundas/<instance-id>/backups?status=FAILED
```

#### Get Details of a Specific Backup

```bash
curl http://localhost:8080/api/camundas/<instance-id>/backups/<backup-id>
```

#### List Incomplete (Cleaned-Up) Backups

Failed backups are automatically moved to an "incomplete" state. To list them:

```bash
curl http://localhost:8080/api/camundas/<instance-id>/backups/incomplete
```

#### List Orphaned Backups

Backups in S3 without corresponding history records:

```bash
curl http://localhost:8080/api/camundas/<instance-id>/backups/orphaned
```

---

### Read Backup Logs

Each backup execution generates a log file with detailed step-by-step output.

```bash
curl http://localhost:8080/api/camundas/<instance-id>/backups/<backup-id>/logs
```

Example output:

```
Backup started at 2024-01-15T10:30:00Z
Trigger type: scheduled
Execution mode: sequential
Backup ID stored in S3
Starting sequential execution of 4 components
Starting backup for component: zeebe
Triggering Zeebe backup
Zeebe backup triggered successfully
Polling Zeebe backup status
Zeebe backup still running (attempt 1/120)
Zeebe backup still running (attempt 2/120)
Zeebe backup completed
Starting backup for component: elasticsearch
Creating Elasticsearch snapshot: bk-20240115-001 (repository: camunda-backup)
Elasticsearch snapshot creation initiated, polling for status...
Elasticsearch snapshot completed successfully
All components completed in sequential mode
Backup completed with status: COMPLETED
Scheduling asynchronous retention policy
Retention policy applied successfully
```

---

## Alert Troubleshooting

Alerts are sent via webhook to the URL configured in `ALERT_WEBHOOK_URL`. If the URL is not set, alerting is disabled.

### Backup Failed (CRITICAL)

**Webhook payload:**
```json
{
  "level": "CRITICAL",
  "title": "Backup Failed",
  "message": "Backup bk-20240115 failed for instance prod-1: failed components: elasticsearch, zeebe",
  "timestamp": "2024-01-15T10:30:45Z",
  "metadata": {
    "instance_id": "prod-1",
    "backup_id": "bk-20240115"
  }
}
```

**Common causes:**
- One or more Camunda components timed out or returned an error.
- Elasticsearch snapshot failed (repository misconfigured, cluster unhealthy).
- S3 storage unreachable during backup ID persistence.
- Network connectivity issues between the controller and Camunda components.

**Remediation:**
1. Check the backup log: `GET /api/camundas/prod-1/backups/bk-20240115/logs`
2. Identify which component(s) failed from the alert message.
3. Check the individual component's health (Zeebe, Operate, Elasticsearch, etc.).
4. Review the [Backup Failures](#backup-failures) section for component-specific guidance.

---

### Cleanup Failed (WARNING)

**Webhook payload:**
```json
{
  "level": "WARNING",
  "title": "Cleanup Failed",
  "message": "Cleanup failed for backup bk-20240115 (instance prod-1): S3 move operation failed",
  "timestamp": "2024-01-15T10:30:45Z",
  "metadata": {
    "instance_id": "prod-1",
    "backup_id": "bk-20240115"
  }
}
```

**What happened:** After a backup failed, the system tried to move the partial backup data to an "incomplete" folder in S3 for later cleanup by the retention manager. This move operation also failed.

**Impact:** The failed backup remains in the main backup path rather than the incomplete path. The retention manager may not clean it up automatically.

**Remediation:**
1. Check S3 connectivity and permissions.
2. Manually move or delete the incomplete backup from S3 if needed.
3. Use the incomplete and orphaned backup listing endpoints to audit:
   ```bash
   curl http://localhost:8080/api/camundas/prod-1/backups/incomplete
   curl http://localhost:8080/api/camundas/prod-1/backups/orphaned
   ```

---

### Stuck Backup Detected (CRITICAL)

**Webhook payload:**
```json
{
  "level": "CRITICAL",
  "title": "Stuck Backup Detected",
  "message": "Backup for instance prod-1 (job prod-1) has been running for 2h5m0s",
  "timestamp": "2024-01-15T12:35:45Z",
  "metadata": {
    "instance_id": "prod-1",
    "job_id": "prod-1",
    "duration": "2h5m0s"
  }
}
```

**What happened:** A scheduled backup job has been running longer than the stuck timeout threshold (default: 2 hours). The alert is sent only once per stuck episode.

**Impact:** No other backups can run while this job holds the global lock.

**Remediation:**
1. Check the backup logs to see where the backup is stalled.
2. Check the health of all Camunda components and Elasticsearch.
3. If the job is truly stuck, restart the service — graceful shutdown will cancel running jobs (with a 5-minute timeout).
4. Consider increasing `BACKUP_STUCK_TIMEOUT_MINUTES` if backups legitimately take longer.

---

### Circuit Breaker Open (WARNING)

**Webhook payload:**
```json
{
  "level": "WARNING",
  "title": "Circuit Breaker Open",
  "message": "Circuit breaker opened for service: elasticsearch",
  "timestamp": "2024-01-15T10:30:45Z",
  "metadata": {
    "service": "elasticsearch"
  }
}
```

**What happened:** 5 consecutive calls to the named service failed, triggering the circuit breaker.

**Impact:** All requests to the affected service are immediately rejected for the next 60 seconds.

**Remediation:**
1. Check the health and availability of the named service.
2. The circuit will auto-recover after 60 seconds if the service is back.
3. See [Circuit Breaker Issues](#circuit-breaker-issues) for detailed guidance.

---

### Scheduler Error (CRITICAL)

**Webhook payload:**
```json
{
  "level": "CRITICAL",
  "title": "Scheduler Error",
  "message": "Critical scheduler error details...",
  "timestamp": "2024-01-15T10:30:45Z",
  "metadata": null
}
```

**What happened:** The scheduler encountered a critical internal error.

**Remediation:**
1. Check the service logs for detailed error information.
2. Verify the readiness endpoint: `GET /readyz`
3. If the scheduler is no longer running, restart the service.

---

### Alerts Not Being Received

If you expect alerts but are not receiving them:

1. Verify the webhook URL is set:
   ```bash
   echo "ALERT_WEBHOOK_URL=$ALERT_WEBHOOK_URL"
   ```
   At startup, the service logs whether alerting is enabled:
   ```
   INFO  Alert webhook configured: notifications enabled
   # or
   INFO  Alert webhook not configured: notifications disabled
   ```
2. Check that your webhook endpoint is reachable from the service. Alert delivery has a 10-second timeout.
3. Look for alert delivery errors in the service logs:
   ```
   ERROR Failed to send alert to https://hooks.slack.com/...: connection refused
   WARN  Alert webhook returned status 403
   ```
4. Alerts are fire-and-forget — delivery failures are logged but never cause the calling operation to fail.

---

## Environment Variable Checklist

Use this checklist to verify your configuration before deployment.

### Required for Production

| Variable | Set? | Notes |
|----------|------|-------|
| `DATA_DIR` | ☐ | Must be a writable directory (default: `/data`) |
| `DEFAULT_S3_ENDPOINT` | ☐ | S3-compatible endpoint URL |
| `DEFAULT_S3_ACCESSKEY` | ☐ | S3 access key |
| `DEFAULT_S3_SECRETKEY` | ☐ | S3 secret key (global default) |

### Per-Instance Credentials

For each Camunda instance, verify credentials are set with the normalized instance ID (uppercase, hyphens → underscores). Example for instance `my-cluster`:

| Variable | Set? | Notes |
|----------|------|-------|
| `ELASTICSEARCH_PASSWORD_MY_CLUSTER` | ☐ | ES password for this instance |
| `S3_SECRETKEY_MY_CLUSTER` | ☐ | S3 secret key for this instance |

### Optional but Recommended

| Variable | Default | Set? | Notes |
|----------|---------|------|-------|
| `PORT` | `8080` | ☐ | HTTP server port |
| `LOG_LEVEL` | `info` | ☐ | `debug`, `info`, `warn`, or `error` |
| `ALERT_WEBHOOK_URL` | _(disabled)_ | ☐ | Webhook for alerts (Slack, PagerDuty, etc.) |
| `BACKUP_STUCK_TIMEOUT_MINUTES` | `120` | ☐ | Minutes before a backup is flagged as stuck (0 = disabled) |

### Elasticsearch Defaults

| Variable | Default | Set? | Notes |
|----------|---------|------|-------|
| `DEFAULT_ELASTICSEARCH_ENDPOINT` | _(none)_ | ☐ | Elasticsearch URL |
| `DEFAULT_ELASTICSEARCH_USERNAME` | _(none)_ | ☐ | Elasticsearch user |
| `DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY` | `camunda-backup` | ☐ | Snapshot repository name |
| `DEFAULT_ELASTICSEARCH_SNAPSHOT_NAME_PREFIX` | _(none)_ | ☐ | Prefix for snapshot names |

### Backup Tuning

| Variable | Default | Set? | Notes |
|----------|---------|------|-------|
| `DEFAULT_SCHEDULE` | `0 2 * * *` | ☐ | Default cron schedule (daily at 2 AM) |
| `DEFAULT_RETENTION_COUNT` | `7` | ☐ | Backups to retain per instance |
| `DEFAULT_BACKUP_POLL_INTERVAL` | `5` | ☐ | Seconds between status polls |
| `DEFAULT_BACKUP_MAX_ATTEMPTS` | `120` | ☐ | Maximum poll attempts before timeout |

### Security Settings

| Variable | Default | Set? | Notes |
|----------|---------|------|-------|
| `PROBE_INSECURE_SKIP_VERIFY` | `false` | ☐ | ⚠️ Skip TLS verification — dev only |
| `PROBE_ALLOW_PRIVATE_IPS` | `false` | ☐ | ⚠️ Allow private IPs — weakens SSRF protection |

---

## FAQ

### How do I trigger a manual backup?

Send a POST request to the backup endpoint:

```bash
curl -X POST http://localhost:8080/api/camundas/<instance-id>/backup
```

The backup runs asynchronously. The response returns immediately with a 202 status and the backup ID.

---

### How do I check if a backup is currently running?

```bash
curl http://localhost:8080/api/status
```

The `active_backups` field indicates the number of running backups (0 or 1, since the system enforces a global concurrency lock).

---

### Can I run multiple backups at the same time?

No. The system enforces a global concurrency lock — only one backup (scheduled or manual) can run at a time. If you attempt to trigger a second backup, you will receive a 409 Conflict response.

---

### How do I change the backup schedule for an instance?

Update the instance configuration:

```bash
curl -X PUT http://localhost:8080/api/camundas/<instance-id> \
  -H "Content-Type: application/json" \
  -d '{"schedule": "0 */6 * * *"}'
```

The scheduler job is updated automatically.

---

### How are per-instance credentials resolved?

The instance ID is normalized to an environment variable suffix by converting to **uppercase** and replacing **hyphens with underscores**:

- Instance `prod-cluster` → suffix `PROD_CLUSTER`
- Instance `my-app` → suffix `MY_APP`

The system then looks up:
- `ELASTICSEARCH_PASSWORD_PROD_CLUSTER`
- `S3_SECRETKEY_PROD_CLUSTER`
- `ELASTICSEARCH_SNAPSHOT_REPOSITORY_PROD_CLUSTER` (falls back to default)
- `ELASTICSEARCH_SNAPSHOT_NAME_PREFIX_PROD_CLUSTER` (falls back to default)

---

### What happens when a backup fails?

1. The failed components are logged in the backup execution record.
2. The partial backup is moved to an "incomplete" folder in S3 (best-effort).
3. A **CRITICAL** alert is sent via webhook (if configured).
4. The retention manager can clean up incomplete backups when newer successful backups exist.

---

### How do I delete a backup?

```bash
curl -X DELETE http://localhost:8080/api/camundas/<instance-id>/backups/<backup-id>
```

**Note:** You cannot delete the most recent successful backup. The system returns a 409 `safety_refusal` error to prevent accidental data loss.

---

### How do I disable stuck backup detection?

```bash
export BACKUP_STUCK_TIMEOUT_MINUTES=0
```

When set to 0, the `checkForStuckJobs()` function returns immediately without checking any jobs.

---

### What happens during a graceful shutdown?

1. The stop signal is sent to the scheduler (no new jobs start).
2. The system waits up to 5 minutes (default `ShutdownTimeout`) for running jobs to finish.
3. If jobs don't finish in time, contexts are cancelled and the service exits.
4. You may see: `Scheduler shutdown timed out, some jobs may still be running`

---

### Why am I seeing "using mock storage" at startup?

```
INFO  S3 credentials not configured, using mock storage
```

This means `DEFAULT_S3_ENDPOINT`, `DEFAULT_S3_ACCESSKEY`, and `DEFAULT_S3_SECRETKEY` are not all set. The service falls back to in-memory mock storage, which is **not persistent** and **not suitable for production**. Set all three variables to use real S3 storage.

---

### How does the retry mechanism work?

The HTTP client retries failed requests with exponential backoff and random jitter:

| Attempt | Base Delay | Jitter Range | Total Wait |
|---------|-----------|--------------|------------|
| 1 | 1s | 0–500ms | 1–1.5s |
| 2 | 2s | 0–1s | 2–3s |
| 3 | 4s | 0–2s | 4–6s |

Retries happen on: network errors, HTTP 408, HTTP 429, and HTTP 5xx responses.
Retries do **not** happen on: HTTP 2xx/3xx/4xx (except 408/429), or context cancellation.
