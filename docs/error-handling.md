# Error Handling and Resilience

This document describes the error handling patterns, resilience mechanisms, logging system, and configuration options available in the Camunda Backup DR tool.

## Table of Contents

- [Structured Errors](#structured-errors)
  - [AppError Type](#apperror-type)
  - [Error Codes](#error-codes)
  - [Sentinel Errors](#sentinel-errors)
  - [Creating Errors](#creating-errors)
- [API Error Responses](#api-error-responses)
- [Circuit Breaker](#circuit-breaker)
- [Retry with Jitter](#retry-with-jitter)
- [Alert Mechanism](#alert-mechanism)
- [Stuck Backup Detection](#stuck-backup-detection)
- [Automatic Cleanup on Failure](#automatic-cleanup-on-failure)
- [Logging](#logging)
- [Configuration Reference](#configuration-reference)

---

## Structured Errors

### AppError Type

For API and handler responses, errors are represented using the `AppError` struct, which provides machine-readable codes, HTTP status mapping, and contextual metadata, while lower-level components may also return sentinel errors for identity checks and internal control flow:

```go
type AppError struct {
    Code       string // Machine-readable error code (e.g., "backup_failed")
    Message    string // Human-readable description
    HTTPStatus int    // HTTP status code (0 defaults to 500)
    Operation  string // Operation that failed (e.g., "ExecuteBackup")
    Component  string // Component involved (e.g., "elasticsearch")
    InstanceID string // Camunda instance ID (e.g., "prod-1")
    Cause      error  // Underlying error (supports error wrapping)
}
```

`AppError` implements the standard `error` interface and supports Go's error wrapping via `Unwrap()`, making it compatible with `errors.Is()` and `errors.As()`.

### Error Codes

| Code | Constant | HTTP Status | Description |
|------|----------|-------------|-------------|
| `backup_failed` | `ErrCodeBackupFailed` | 500 | Backup operation failed |
| `circuit_open` | `ErrCodeCircuitOpen` | 503 | Circuit breaker is open; service unavailable |
| `cleanup_failed` | `ErrCodeCleanupFailed` | 500 | Post-failure cleanup failed |
| `not_found` | `ErrCodeNotFound` | 404 | Requested resource not found |
| `validation_error` | `ErrCodeValidation` | 400 | Request validation failed |
| `external_call_failed` | `ErrCodeExternalCall` | 502 | External service call failed |
| `retry_exhausted` | `ErrCodeRetryExhausted` | 500 | All retry attempts failed |
| `timeout` | `ErrCodeTimeout` | 504 | Operation timed out |

### Sentinel Errors

These are simple error values used for identity checks with `errors.Is()`:

| Error | Message |
|-------|---------|
| `ErrCamundaInstanceNotFound` | `camunda instance not found` |
| `ErrCamundaInstanceAlreadyExists` | `camunda instance already exists` |
| `ErrInvalidConfiguration` | `invalid configuration` |
| `ErrBackupInProgress` | `backup already in progress` |
| `ErrBackupNotFound` | `backup not found` |
| `ErrS3ConnectionFailed` | `failed to connect to S3` |
| `ErrElasticsearchConnectionFailed` | `failed to connect to Elasticsearch` |
| `ErrInvalidComponent` | `invalid component` |
| `ErrNoComponentsEnabled` | `no components enabled for backup` |
| `ErrBackupFailed` | `backup failed` |
| `ErrFileStorageFailed` | `file storage operation failed` |
| `ErrInvalidCamundaInstance` | `invalid camunda instance configuration` |
| `ErrCannotDeleteMostRecentBackup` | `cannot delete the most recent successful backup` |
| `ErrCircuitBreakerOpen` | `circuit breaker is open` |
| `ErrBackupStuck` | `backup appears stuck` |
| `ErrCleanupFailed` | `cleanup after failure encountered errors` |
| `ErrRetryExhausted` | `all retry attempts exhausted` |

### Creating Errors

**Convenience constructors** (recommended):

```go
// Validation error (400)
err := utils.NewValidationError("instance name must contain only lowercase letters and hyphens")

// Not found (404)
err := utils.NewNotFoundError("camunda instance not found: prod-1")

// External call failure (502)
err := utils.NewExternalCallError("elasticsearch snapshot failed", cause)

// Backup failure (500)
err := utils.NewBackupFailedError("backup failed due to component timeout", cause)

// Timeout (504)
err := utils.NewTimeoutError("backup polling exceeded maximum attempts", cause)

// Circuit breaker open (503)
err := utils.NewCircuitOpenError("elasticsearch")

// Cleanup failure (500)
err := utils.NewCleanupFailedError("failed to move backup to incomplete", cause)
```

**Adding context** (uses copy semantics — safe to chain):

```go
err := utils.NewBackupFailedError("snapshot creation failed", cause).
    WithOperation("ExecuteBackup").
    WithComponent("elasticsearch").
    WithInstance("prod-1")
```

**Wrapping an existing error:**

```go
err := utils.WrapError(originalErr, "backup_failed", "backup operation failed", http.StatusInternalServerError)
```

---

## API Error Responses

All API errors are returned as JSON with a consistent structure:

```json
{
  "error": "validation_error",
  "message": "instance name must contain only lowercase letters and hyphens",
  "code": 400
}
```

When an `AppError` includes component or instance context, additional fields are included:

```json
{
  "error": "backup_failed",
  "message": "backup failed due to elasticsearch timeout",
  "code": 500,
  "component": "elasticsearch",
  "instance_id": "prod-1"
}
```

For non-`AppError` errors (unexpected internal errors), the response uses a generic message to avoid leaking internal details:

```json
{
  "error": "internal_error",
  "message": "an internal error occurred",
  "code": 500
}
```

### Error Response Fields

| Field | Type | Always Present | Description |
|-------|------|----------------|-------------|
| `error` | string | Yes | Machine-readable error code |
| `message` | string | Yes | Human-readable description |
| `code` | int | Yes | HTTP status code |
| `component` | string | No | Component that caused the error |
| `instance_id` | string | No | Related Camunda instance ID |

### Common API Error Responses

| Endpoint | Error Code | Status | When |
|----------|-----------|--------|------|
| `POST /api/camundas` | `validation_error` | 400 | Invalid instance configuration |
| `POST /api/camundas` | `validation_error` | 409 | Instance ID already exists |
| `POST /api/camundas/{id}/backup` | `not_found` | 404 | Instance not found |
| `POST /api/camundas/{id}/backup` | `backup_failed` | 500 | Internal backup trigger error |
| Any endpoint | `circuit_open` | 503 | Circuit breaker is open for a dependency |
| Any endpoint | `external_call_failed` | 502 | Upstream service returned an error |

### Panic Recovery

The recovery middleware catches panics in HTTP handlers. It uses `errors.As` to extract an `*AppError` from the panic value (including wrapped errors). If found, it returns the structured error response. Otherwise, it returns a generic `500 Internal Server Error`.

---

## Circuit Breaker

The circuit breaker prevents cascading failures by temporarily blocking calls to failing external services. The `utils.CircuitBreaker` component is fully implemented and tested, but is not yet wired into the main external-call paths. This section describes the intended behavior for when it is integrated into production flows.

### States

```
┌──────────┐  MaxFailures reached  ┌──────────┐  ResetTimeout elapsed  ┌───────────┐
│  CLOSED  │ ────────────────────► │   OPEN   │ ─────────────────────► │ HALF_OPEN │
│ (normal) │                       │ (reject) │                        │  (probe)  │
└──────────┘ ◄──────────────────── └──────────┘                        └───────────┘
                success resets           ▲            success ──► CLOSED
                failure count            │            failure ──► OPEN
                                         └──────────────────────────────────┘
```

- **CLOSED** — Normal operation. All requests pass through. Each failure increments the counter; each success resets it to zero.
- **OPEN** — Too many failures. All requests are immediately rejected with an error wrapping the sentinel `ErrCircuitBreakerOpen`. To return HTTP 503 from handlers, callers should wrap this into an `*AppError` via `utils.NewCircuitOpenError()` so that `ToHTTPError` maps it to 503.
- **HALF_OPEN** — Recovery probe. A limited number of requests are allowed through. A single success closes the circuit; any failure re-opens it.

### Default Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MaxFailures` | 5 | Consecutive failures before the circuit opens |
| `ResetTimeout` | 60s | Time in OPEN state before transitioning to HALF_OPEN |
| `HalfOpenMaxCalls` | 1 | Number of probe calls allowed in HALF_OPEN |

### State Change Callbacks

Register a callback to be notified of state transitions (fired asynchronously):

```go
cb := utils.NewCircuitBreaker("elasticsearch", utils.DefaultCircuitBreakerConfig())
cb.OnStateChange(func(name string, from, to utils.CircuitState) {
    log.Printf("Circuit %s: %s → %s", name, from, to)
})
```

### Manual Reset

Force the circuit back to CLOSED:

```go
cb.Reset()
```

---

## Retry with Jitter

The HTTP client automatically retries failed requests using **exponential backoff with random jitter** to prevent thundering herd problems.

### Default Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MaxRetries` | 3 | Maximum retry attempts |
| `RetryDelay` | 1s | Initial delay before first retry |
| `MaxRetryDelay` | 30s | Maximum delay cap |
| `Timeout` | 30s | Per-request timeout |

### Backoff Formula

For each retry attempt:

```
if delay > 2ns:
    jitter = random value in [0, delay/2)
else:
    jitter = 0
waitTime = delay + jitter
delay    = min(delay × 2, MaxRetryDelay)    // for next attempt
```

When the delay is very small (< 2 nanoseconds), jitter is skipped to avoid edge cases.

**Example progression** (3 retries):

| Attempt | Base Delay | Jitter Range | Total Wait |
|---------|-----------|--------------|------------|
| 1 | 1s | [0, 500ms) | 1–1.5s |
| 2 | 2s | [0, 1s) | 2–3s |
| 3 | 4s | [0, 2s) | 4–6s |

### Retryable Conditions

Requests are retried on:
- Network errors (connection refused, DNS failure, etc.)
- HTTP 408 (Request Timeout)
- HTTP 429 (Too Many Requests)
- HTTP 5xx (Server Errors)

Requests are **not** retried on:
- HTTP 2xx, 3xx, or 4xx (except 408 and 429)
- Context cancellation (`context.Canceled`, `context.DeadlineExceeded`)

---

## Alert Mechanism

The alerting system sends webhook notifications for critical events. It is **fire-and-forget** — alert delivery failures are logged but never propagate errors to the caller.

### Enabling Alerts

Set the `ALERT_WEBHOOK_URL` environment variable to your webhook endpoint:

```bash
export ALERT_WEBHOOK_URL="https://hooks.slack.com/services/T00/B00/xxx"
```

When `ALERT_WEBHOOK_URL` is empty (the default), all alert methods are no-ops.

### Webhook Payload

Alerts are sent as `POST` requests with a JSON body:

```json
{
  "level": "CRITICAL",
  "title": "Backup Failed",
  "message": "Backup bk-20240115 failed for instance prod-1: elasticsearch snapshot timeout",
  "timestamp": "2024-01-15T10:30:45Z",
  "metadata": {
    "instance_id": "prod-1",
    "backup_id": "bk-20240115"
  }
}
```

### Alert Levels

| Level | Constant | Used For |
|-------|----------|----------|
| `INFO` | `AlertInfo` | Informational notices |
| `WARNING` | `AlertWarning` | Non-critical issues (cleanup failures, circuit open) |
| `CRITICAL` | `AlertCritical` | Backup failures, stuck backups, scheduler errors |

### Built-in Alerts

| Alert | Level | Trigger | Metadata |
|-------|-------|---------|----------|
| Backup Failed | CRITICAL | Backup finishes with FAILED status | `instance_id`, `backup_id` |
| Cleanup Failed | WARNING | Failed to move incomplete backup to S3 | `instance_id`, `backup_id` |
| Stuck Backup | CRITICAL | Running job exceeds stuck timeout | `instance_id`, `job_id`, `duration` |
| Circuit Open | WARNING | Circuit breaker transitions to OPEN | `service` |
| Scheduler Error | CRITICAL | Critical scheduler error | — |

### Behavior Notes

- **HTTP timeout**: 10 seconds per webhook call
- **Async delivery**: Alerts are sent in a goroutine and do not block the caller
- **Error resilience**: Webhook delivery errors are logged but never returned
- **Disabled mode**: When `ALERT_WEBHOOK_URL` is empty, `SendAlert()` returns immediately

---

## Stuck Backup Detection

The scheduler monitors running backup jobs and raises alerts when a job exceeds the configured timeout.

### How It Works

1. When a backup job starts, the scheduler records a `RunningStartedAt` timestamp.
2. On every scheduler tick (default: 1 minute), `checkForStuckJobs()` runs.
3. For each running job, if `time.Since(RunningStartedAt) >= StuckTimeout`, the job is flagged as stuck.
4. A `CRITICAL` alert is sent and an error is logged.
5. The alert is **sent only once** per stuck episode — the job's `StuckAlertedAt` timestamp prevents duplicate alerts on subsequent ticks. It resets when the job finishes.

### Important Notes

- **Detection only** — stuck backup detection does **not** kill or cancel the running job. It raises visibility so operators can investigate.
- **Deduplicated** — Each stuck job triggers a single alert. Subsequent scheduler ticks do not re-alert for the same stuck episode.
- **Disable** — Set `BACKUP_STUCK_TIMEOUT_MINUTES=0` to disable detection.
- **Default** — 120 minutes (2 hours).

---

## Automatic Cleanup on Failure

When a backup completes with `FAILED` status, the orchestrator automatically performs cleanup:

1. **Identifies failed components** — Collects the list of components that reported failure.
2. **Moves to incomplete** — Calls `s3Storage.MoveToIncomplete(instanceID, backupID)` to move the partial backup out of the main backup path. The retention manager can clean these up when newer successful backups exist.
3. **Sends failure alert** — Fires a `CRITICAL` alert with the instance ID, backup ID, and failure reason.

### Error Handling During Cleanup

Cleanup is designed to be **best-effort and non-blocking**:
- If `MoveToIncomplete` fails, the error is logged and a `WARNING` alert is sent, but the process continues.
- If alert delivery fails, the error is logged but does not propagate.
- Cleanup errors never affect the overall backup result reporting.

---

## Logging

### Log Levels

| Level | Output | When |
|-------|--------|------|
| `ERROR` | stderr | Always enabled. Errors that need attention. |
| `WARN` | stdout | Always enabled. Non-critical issues. |
| `INFO` | stdout | Always enabled. Normal operational messages. |
| `DEBUG` | stdout | Only when `LOG_LEVEL=debug`. Verbose troubleshooting output. |

### Setting the Log Level

```bash
# Enable debug logging
export LOG_LEVEL=debug

# Default (info level — debug messages suppressed)
export LOG_LEVEL=info
```

Valid values: `debug`, `info`, `warn`, `error`. Default: `info`.

When `LOG_LEVEL` is not `debug`, debug messages are discarded (written to `io.Discard`).

### Contextual Logging

Use `WithContext()` to add operation, component, and instance context to log messages:

```go
ctxLog := logger.WithContext("ExecuteBackup", "elasticsearch", "prod-1")
ctxLog.Info("Starting snapshot")
// Output: INFO  [op=ExecuteBackup component=elasticsearch instance=prod-1] Starting snapshot
```

Parameters are optional — only non-empty values are included in the prefix:

```go
logger.WithContext("", "", "prod-1").Warn("connection slow")
// Output: WARN  [instance=prod-1] connection slow
```

### Backup-Scoped Logging

Use `WithBackupID()` for backup-specific log context:

```go
bkLog := logger.WithBackupID("bk-20240115-001")
bkLog.Info("Backup started")
// Output: INFO  [BACKUP_ID: bk-20240115-001] Backup started
```

### Log Format

All log messages include the date, time, and source file:

```
INFO  2024/01/15 10:30:45 orchestrator.go:142: [op=ExecuteBackup instance=prod-1] Backup started
ERROR 2024/01/15 10:31:02 orchestrator.go:195: [op=ExecuteBackup instance=prod-1] Snapshot failed: connection refused
```

---

## Configuration Reference

### Service Settings

| Environment Variable | Type | Default | Description |
|---------------------|------|---------|-------------|
| `PORT` | int | `8080` | HTTP server port |
| `LOG_LEVEL` | string | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `DATA_DIR` | string | `/data` | Data directory for local file storage |

### Default Backup Settings

| Environment Variable | Type | Default | Description |
|---------------------|------|---------|-------------|
| `DEFAULT_SCHEDULE` | string | `0 2 * * *` | Default cron schedule (2:00 AM daily) |
| `DEFAULT_RETENTION_COUNT` | int | `7` | Number of backups to retain |
| `DEFAULT_SUCCESS_HISTORY` | int | `30` | Days to keep success history records |
| `DEFAULT_FAILURE_HISTORY` | int | `30` | Days to keep failure history records |

### Backup Polling

| Environment Variable | Type | Default | Description |
|---------------------|------|---------|-------------|
| `DEFAULT_BACKUP_POLL_INTERVAL` | int | `5` | Seconds between backup status polls |
| `DEFAULT_BACKUP_MAX_ATTEMPTS` | int | `120` | Maximum polling attempts before timeout |

### Default Elasticsearch Settings

| Environment Variable | Type | Default | Description |
|---------------------|------|---------|-------------|
| `DEFAULT_ELASTICSEARCH_ENDPOINT` | string | — | Elasticsearch URL. Pre-populates new instance forms. |
| `DEFAULT_ELASTICSEARCH_USERNAME` | string | — | Elasticsearch username. Pre-populates new instance forms. |
| `DEFAULT_ELASTICSEARCH_PASSWORD` | string | — | Global fallback ES password (used when no instance-specific var is set) |
| `DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY` | string | `camunda-backup` | Snapshot repository name |
| `DEFAULT_ELASTICSEARCH_SNAPSHOT_NAME_PREFIX` | string | — | Snapshot name prefix |

### Default S3 Settings

| Environment Variable | Type | Default | Description |
|---------------------|------|---------|-------------|
| `DEFAULT_S3_ENDPOINT` | string | _(required)_ | S3-compatible endpoint URL. Required for startup. |
| `DEFAULT_S3_ACCESSKEY` | string | _(required)_ | S3 access key ID. Required for startup. |
| `DEFAULT_S3_SECRETKEY` | string | _(required)_ | S3 secret access key. Falls back per-instance via `S3_SECRETKEY_<ID>`. |
| `DEFAULT_S3_BUCKET` | string | `camunda-backups` | S3 bucket name for backup history and IDs. |
| `DEFAULT_S3_REGION` | string | `us-east-1` | AWS region for the S3 bucket. |
| `DEFAULT_S3_PREFIX` | string | _(empty)_ | Key prefix inside the bucket. |
| `DEFAULT_S3_USE_PATH_STYLE` | string | `true` | Path-style addressing. Required for MinIO; set `false` for AWS S3. |

### Alert Settings

| Environment Variable | Type | Default | Description |
|---------------------|------|---------|-------------|
| `ALERT_WEBHOOK_URL` | string | — (disabled) | Webhook URL for alert notifications. Leave empty to disable. |
| `BACKUP_STUCK_TIMEOUT_MINUTES` | int | `120` | Minutes before a running backup is considered stuck. Set to `0` to disable. |

### Per-Instance Credentials

Credentials can be set per Camunda instance using environment variables. The instance ID is normalized: converted to **uppercase** with **hyphens replaced by underscores**.

For example, instance `my-cluster` uses the suffix `MY_CLUSTER`.

| Environment Variable Pattern | Description |
|-----------------------------|-------------|
| `ELASTICSEARCH_PASSWORD_<ID>` | Elasticsearch password for the instance (falls back to `DEFAULT_ELASTICSEARCH_PASSWORD`) |
| `ELASTICSEARCH_SNAPSHOT_REPOSITORY_<ID>` | Snapshot repository override (falls back to default) |
| `ELASTICSEARCH_SNAPSHOT_NAME_PREFIX_<ID>` | Snapshot name prefix override (falls back to default) |
| `S3_SECRETKEY_<ID>` | S3 secret key for the instance (falls back to `DEFAULT_S3_SECRETKEY`) |

**Example:**

```bash
# For instance "prod-cluster"
export ELASTICSEARCH_PASSWORD_PROD_CLUSTER="secret123"
export S3_SECRETKEY_PROD_CLUSTER="s3secret456"
```

### Endpoint Security Settings

| Environment Variable | Type | Default | Description |
|---------------------|------|---------|-------------|
| `PROBE_INSECURE_SKIP_VERIFY` | bool | `false` | Skip TLS certificate verification for endpoint probes |
| `PROBE_ALLOW_PRIVATE_IPS` | bool | `false` | Allow endpoint probes to private/loopback IP addresses (SSRF protection) |

> **⚠️ Warning:** Both `PROBE_INSECURE_SKIP_VERIFY` and `PROBE_ALLOW_PRIVATE_IPS` weaken security and should only be used in development or trusted environments.
