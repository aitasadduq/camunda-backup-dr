# Camunda Backup DR — API Reference

## Overview

The Camunda Backup DR controller exposes a JSON REST API for managing Camunda cluster instances, triggering and monitoring backups, reviewing backup history, and performing retention cleanup.

**Base URL:** `http://<host>:<port>` (default port is configured at startup)

All API endpoints are prefixed with `/api/` except for the health/readiness probes (`/healthz`, `/readyz`).

**Content Type:** All request and response bodies use `application/json` unless otherwise noted.

---

## Authentication

The API does **not** include built-in authentication or authorization. It is designed to run behind a network-level security boundary (VPN, private network, reverse proxy with auth, etc.). Ensure the service is not exposed to untrusted networks.

---

## CSRF Protection

All **state-changing** requests (`POST`, `PUT`, `DELETE`) to `/api/*` endpoints require the following header:

```
X-Requested-With: XMLHttpRequest
```

Requests without this header receive a `403 Forbidden` response:

```json
{
  "error": "csrf_rejected",
  "message": "Missing or invalid X-Requested-With header",
  "code": 403
}
```

Safe methods (`GET`, `HEAD`, `OPTIONS`) and non-API routes are exempt. This header-based check works in conjunction with the CORS policy to prevent cross-site request forgery attacks from browsers.

---

## Error Response Format

All error responses follow a consistent structure:

```json
{
  "error": "<error_type>",
  "message": "<human-readable description>",
  "code": <http_status_code>,
  "component": "<component_name>",
  "instance_id": "<instance_id>"
}
```

| Field | Type | Description |
|---|---|---|
| `error` | string | Machine-readable error type (e.g., `not_found`, `validation_error`) |
| `message` | string | Human-readable error description |
| `code` | integer | HTTP status code |
| `component` | string | *(optional)* The Camunda component related to the error |
| `instance_id` | string | *(optional)* The instance ID related to the error |

### Common Error Types

| Error Type | HTTP Code | Description |
|---|---|---|
| `validation_error` | 400 | Invalid input or missing required fields |
| `invalid_request` | 400 | Malformed JSON body |
| `csrf_rejected` | 403 | Missing or invalid `X-Requested-With` header |
| `not_found` | 404 | Resource not found |
| `method_not_allowed` | 405 | HTTP method not supported for this endpoint |
| `conflict` | 409 | Resource already exists or concurrent operation |
| `backup_in_progress` | 409 | A backup is already running for this instance |
| `safety_refusal` | 409 | Operation refused for safety (e.g., deleting the most recent backup) |
| `internal_error` | 500 | Unexpected server error |

---

## Success Response Format

Mutating operations that return a success message use:

```json
{
  "message": "<success description>",
  "data": <object or null>
}
```

---

## Endpoints

### System / Health

#### `GET /healthz` — Liveness Probe

Returns whether the service process is alive. Suitable for Kubernetes liveness probes.

**Response:** `200 OK`

```json
{
  "status": "healthy",
  "checks": {
    "service": "ok"
  },
  "timestamp": "2024-01-15T14:30:52Z"
}
```

---

#### `GET /readyz` — Readiness Probe

Returns whether the service is ready to accept traffic. Checks scheduler and configuration manager availability. Suitable for Kubernetes readiness probes.

**Response:** `200 OK` or `503 Service Unavailable`

```json
{
  "status": "ready",
  "checks": {
    "scheduler": "running",
    "camunda_manager": "ok"
  },
  "timestamp": "2024-01-15T14:30:52Z"
}
```

If not ready:

```json
{
  "status": "not_ready",
  "checks": {
    "scheduler": "not_running",
    "camunda_manager": "error"
  },
  "timestamp": "2024-01-15T14:30:52Z"
}
```

---

#### `GET /api/status` — System Status

Returns an aggregate view of the system: scheduler state, storage health, instance counts, and active backup count.

**Response:** `200 OK`

```json
{
  "status": "ok",
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
  "active_backups": 0,
  "timestamp": "2024-01-15T14:30:52Z"
}
```

---

### Instance Management

#### `GET /api/camundas` — List All Instances

Returns all configured Camunda instances.

**Response:** `200 OK`

```json
[
  {
    "id": "prod-cluster",
    "name": "Production Cluster",
    "base_url": "https://camunda.prod.example.com",
    "enabled": true,
    "schedule": "0 2 * * *",
    "success_retention": 7,
    "failure_retention": 7,
    "zeebe_backup_endpoint": "https://zeebe.prod.example.com:9600",
    "operate_backup_endpoint": "https://operate.prod.example.com",
    "tasklist_backup_endpoint": "https://tasklist.prod.example.com",
    "optimize_backup_endpoint": "",
    "components": [
      { "name": "zeebe", "enabled": true },
      { "name": "operate", "enabled": true },
      { "name": "tasklist", "enabled": true },
      { "name": "optimize", "enabled": false },
      { "name": "elasticsearch", "enabled": true }
    ],
    "parallel_execution": false,
    "elasticsearch_endpoint": "https://es.prod.example.com:9200",
    "elasticsearch_username": "elastic",
    "s3_endpoint": "https://s3.us-east-1.amazonaws.com",
    "s3_accesskey": "AKIAIOSFODNN7EXAMPLE",
    "elasticsearch_password_env_var": "ELASTICSEARCH_PASSWORD_PROD_CLUSTER",
    "s3_secret_key_env_var": "S3_SECRETKEY_PROD_CLUSTER",
    "created_at": "2024-01-10T09:00:00Z",
    "updated_at": "2024-01-15T14:30:52Z",
    "last_backup_at": "2024-01-15T02:00:00Z",
    "last_backup_status": "COMPLETED"
  }
]
```

| Error Code | Condition |
|---|---|
| 500 | Failed to list instances |

---

#### `POST /api/camundas` — Create Instance

Creates a new Camunda instance configuration. The ID is automatically lowercased.

**Headers:** `X-Requested-With: XMLHttpRequest`

**Request Body:**

```json
{
  "id": "prod-cluster",
  "name": "Production Cluster",
  "base_url": "https://camunda.prod.example.com",
  "schedule": "0 2 * * *",
  "success_retention": 7,
  "failure_retention": 7,
  "zeebe_backup_endpoint": "https://zeebe.prod.example.com:9600",
  "operate_backup_endpoint": "https://operate.prod.example.com",
  "tasklist_backup_endpoint": "https://tasklist.prod.example.com",
  "optimize_backup_endpoint": "",
  "components": [
    { "name": "zeebe", "enabled": true },
    { "name": "operate", "enabled": true },
    { "name": "tasklist", "enabled": true },
    { "name": "optimize", "enabled": false },
    { "name": "elasticsearch", "enabled": true }
  ],
  "parallel_execution": false,
  "elasticsearch_endpoint": "https://es.prod.example.com:9200",
  "elasticsearch_username": "elastic",
  "s3_endpoint": "https://s3.us-east-1.amazonaws.com",
  "s3_accesskey": "AKIAIOSFODNN7EXAMPLE",
  "notifications": {
    "on_success": {
      "enabled": true,
      "method": "POST",
      "url": "https://hooks.example.com:8443/backup-events",
      "body": "{\"channel\": \"backups\"}",
      "message_field": "text"
    },
    "on_failure": {
      "enabled": false
    }
  }
}
```

**Required fields:** `id`, `name`, `base_url`, `s3_endpoint`, `s3_accesskey`

**Defaults applied when omitted:**

| Field | Default |
|---|---|
| `schedule` | `0 2 * * *` (daily at 2 AM) |
| `success_retention` | `7` |
| `failure_retention` | `7` |
| `components` | zeebe ✓, operate ✓, tasklist ✓, optimize ✗, elasticsearch ✓ |
| `enabled` | `true` |

**ID format:** Lowercase letters and hyphens only. Must start and end with a letter (e.g., `prod-cluster`, `staging`, `a`).

**Response:** `201 Created`

```json
{
  "message": "Camunda instance created successfully",
  "data": {
    "id": "prod-cluster",
    "name": "Production Cluster",
    "base_url": "https://camunda.prod.example.com",
    "enabled": true,
    "schedule": "0 2 * * *",
    "success_retention": 7,
    "failure_retention": 7,
    "components": [
      { "name": "zeebe", "enabled": true },
      { "name": "operate", "enabled": true },
      { "name": "tasklist", "enabled": true },
      { "name": "optimize", "enabled": false },
      { "name": "elasticsearch", "enabled": true }
    ],
    "parallel_execution": false,
    "s3_endpoint": "https://s3.us-east-1.amazonaws.com",
    "s3_accesskey": "AKIAIOSFODNN7EXAMPLE",
    "elasticsearch_password_env_var": "ELASTICSEARCH_PASSWORD_PROD_CLUSTER",
    "s3_secret_key_env_var": "S3_SECRETKEY_PROD_CLUSTER",
    "created_at": "2024-01-15T14:30:52Z",
    "updated_at": "2024-01-15T14:30:52Z",
    "last_backup_status": "NEVER_BACKED_UP"
  }
}
```

| Error Code | Condition |
|---|---|
| 400 | Missing required field or invalid configuration |
| 409 | Instance with this ID already exists |
| 500 | Internal server error |

---

#### Notifications

Each instance can send an HTTP request of its own when one of its backups
finishes. The request is fully described by the user: the method, the endpoint,
and — both optional — the body and the field inside that body where the
controller writes its message.

| Field | Meaning |
|---|---|
| `enabled` | Whether this request is sent at all. A disabled request is never validated, so a half-filled form still saves. |
| `method` | `GET`, `POST`, `PUT`, `PATCH` or `DELETE`. Defaults to `POST`. |
| `url` | The endpoint, including the port when it is not the scheme default. `http` and `https` only. |
| `body` | Optional, but when given it must be a JSON **object** — whether or not `message_field` names a place inside it. |
| `message_field` | Optional. Where in `body` the controller writes its message. A dotted path nests: `payload.text` produces `{"payload": {"text": "..."}}`. Missing intermediate objects are created; an existing non-object in the path is an error. |

Both are optional, which gives four shapes:

| `body` | `message_field` | What is sent |
|---|---|---|
| set | set | the body with the message written into that field |
| empty | set | just the message in that field: `{"text": "..."}` |
| set | empty | the body byte for byte as it was written, key order and formatting intact |
| empty | empty | no body and no `Content-Type`: the request arriving is the whole signal |

Anything with a body is sent as `application/json`. The body is validated the
same way in every shape, so a body that saves with the message field blank still
saves once it is filled in — a trailing comma is rejected either way rather than
slipping through in one case and failing in the other.

`on_success` fires for a `COMPLETED` backup. `on_failure` fires for `FAILED` and
for `INCOMPLETE` — an interrupted backup left artifacts nobody asked for, so it
is reported as a failure rather than passed over in silence. A backup that never
reached a terminal state notifies nothing.

The body, when there is one, is sent whatever the method, and the request is
given 15 seconds. A notification that cannot be delivered is logged and dropped:
the backup has already finished, and its recorded result never depends on
whether the endpoint answered.

**Example.** With `body` `{"channel": "backups"}` and `message_field` `payload.text`,
a successful backup sends:

```json
{
  "channel": "backups",
  "payload": {
    "text": "Backup 20260910020000 of Camunda instance Production Cluster (prod-cluster) completed successfully."
  }
}
```

A failed one sends the same shape with the failure in place of the message:

```json
{
  "channel": "backups",
  "payload": {
    "text": "Backup 20260910020000 of Camunda instance Production Cluster (prod-cluster) finished with status FAILED: failed components: operate, zeebe"
  }
}
```

An enabled notification whose endpoint resolves to a private or loopback address
is rejected with `400`, the same guard that applies to `exporting_endpoint`. Set
`PROBE_ALLOW_PRIVATE_IPS=true` to notify a service inside the cluster.

#### `GET /api/camundas/{id}` — Get Instance

Returns a single Camunda instance by ID.

**Response:** `200 OK`

```json
{
  "id": "prod-cluster",
  "name": "Production Cluster",
  "base_url": "https://camunda.prod.example.com",
  "enabled": true,
  "schedule": "0 2 * * *",
  "success_retention": 7,
  "failure_retention": 7,
  "components": [
    { "name": "zeebe", "enabled": true },
    { "name": "operate", "enabled": true },
    { "name": "tasklist", "enabled": true },
    { "name": "optimize", "enabled": false },
    { "name": "elasticsearch", "enabled": true }
  ],
  "parallel_execution": false,
  "elasticsearch_endpoint": "https://es.prod.example.com:9200",
  "elasticsearch_username": "elastic",
  "s3_endpoint": "https://s3.us-east-1.amazonaws.com",
  "s3_accesskey": "AKIAIOSFODNN7EXAMPLE",
  "elasticsearch_password_env_var": "ELASTICSEARCH_PASSWORD_PROD_CLUSTER",
  "s3_secret_key_env_var": "S3_SECRETKEY_PROD_CLUSTER",
  "created_at": "2024-01-10T09:00:00Z",
  "updated_at": "2024-01-15T14:30:52Z",
  "last_backup_at": "2024-01-15T02:00:00Z",
  "last_backup_status": "COMPLETED"
}
```

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing or invalid |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `PUT /api/camundas/{id}` — Update Instance

Updates an existing Camunda instance. Provide only the fields you want to change.

**Headers:** `X-Requested-With: XMLHttpRequest`

**Request Body:**

```json
{
  "name": "Production Cluster (Updated)",
  "schedule": "0 3 * * *",
  "success_retention": 14,
  "failure_retention": 14,
  "enabled": true,
  "components": [
    { "name": "zeebe", "enabled": true },
    { "name": "operate", "enabled": true },
    { "name": "tasklist", "enabled": true },
    { "name": "optimize", "enabled": true },
    { "name": "elasticsearch", "enabled": true }
  ]
}
```

**Response:** `200 OK`

```json
{
  "message": "Camunda instance updated successfully",
  "data": null
}
```

| Error Code | Condition |
|---|---|
| 400 | Invalid JSON body or invalid configuration |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `DELETE /api/camundas/{id}` — Delete Instance

Permanently removes a Camunda instance configuration and deregisters its scheduler job.

**Headers:** `X-Requested-With: XMLHttpRequest`

**Response:** `200 OK`

```json
{
  "message": "Camunda instance deleted successfully",
  "data": null
}
```

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `POST /api/camundas/{id}/enable` — Enable Instance

Enables scheduled backups for the instance and updates the scheduler job.

**Headers:** `X-Requested-With: XMLHttpRequest`

**Response:** `200 OK`

```json
{
  "message": "Camunda instance enabled successfully",
  "data": null
}
```

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `POST /api/camundas/{id}/disable` — Disable Instance

Disables scheduled backups for the instance. Manual backups can still be triggered.

**Headers:** `X-Requested-With: XMLHttpRequest`

**Response:** `200 OK`

```json
{
  "message": "Camunda instance disabled successfully",
  "data": null
}
```

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 500 | Internal server error |

---

### Backup Operations

#### `POST /api/camundas/{id}/backup` — Trigger Manual Backup

Triggers an immediate backup for the specified Camunda instance. The backup runs asynchronously in the background. Only one backup may run at a time per system; concurrent requests receive a `409` conflict.

**Headers:** `X-Requested-With: XMLHttpRequest`

**Response:** `202 Accepted`

```json
{
  "message": "Backup triggered successfully",
  "backup_id": "20240115143052",
  "status": "RUNNING"
}
```

The `backup_id` is a timestamp-based identifier in `YYYYMMDDHHMMSS` format. Use it to query backup details and logs.

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 409 | A backup is already in progress |
| 500 | Internal server error |

---

### Backup History

#### `GET /api/camundas/{id}/backups` — List Backup History

Returns the backup history for a Camunda instance, ordered by most recent first.

**Query Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `status` | string | *(optional)* Filter by backup status: `RUNNING`, `COMPLETED`, `FAILED`, `INCOMPLETE` |

**Example:** `GET /api/camundas/prod-cluster/backups?status=completed`

**Response:** `200 OK`

```json
[
  {
    "backup_id": "20240115020000",
    "camunda_instance_id": "prod-cluster",
    "camunda_instance_name": "Production Cluster",
    "start_time": "2024-01-15T02:00:00Z",
    "end_time": "2024-01-15T02:12:34Z",
    "duration_seconds": 754,
    "status": "COMPLETED",
    "trigger_type": "SCHEDULED",
    "components": {
      "zeebe": {
        "enabled": true,
        "status": "COMPLETED",
        "start_time": "2024-01-15T02:00:01Z",
        "end_time": "2024-01-15T02:05:23Z",
        "duration_seconds": 322,
        "snapshot_name": "camunda_zeebe_20240115020000",
        "snapshot_repository": "camunda-backups"
      },
      "operate": {
        "enabled": true,
        "status": "COMPLETED",
        "start_time": "2024-01-15T02:05:24Z",
        "end_time": "2024-01-15T02:08:10Z",
        "duration_seconds": 166,
        "snapshot_name": "camunda_operate_20240115020000",
        "snapshot_repository": "camunda-backups"
      },
      "tasklist": {
        "enabled": true,
        "status": "COMPLETED",
        "start_time": "2024-01-15T02:08:11Z",
        "end_time": "2024-01-15T02:10:45Z",
        "duration_seconds": 154,
        "snapshot_name": "camunda_tasklist_20240115020000",
        "snapshot_repository": "camunda-backups"
      },
      "optimize": {
        "enabled": false,
        "status": "SKIPPED",
        "duration_seconds": 0
      },
      "elasticsearch": {
        "enabled": true,
        "status": "COMPLETED",
        "start_time": "2024-01-15T02:10:46Z",
        "end_time": "2024-01-15T02:12:33Z",
        "duration_seconds": 107,
        "snapshot_name": "camunda_es_20240115020000",
        "snapshot_repository": "camunda-backups"
      }
    },
    "backup_stats": {
      "total_components": 4,
      "successful_components": 4,
      "failed_components": 0,
      "skipped_components": 1
    },
    "metadata": {
      "config_version": "1.0",
      "controller_version": "0.5.0",
      "execution_mode": "sequential",
      "log_file_path": "data/logs/prod-cluster/20240115020000.log",
      "backup_reason": "Scheduled backup"
    }
  }
]
```

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `GET /api/camundas/{id}/backups/{backupId}` — Get Backup Details

> **Reserved names.** `orphaned`, `incomplete`, `failed`, `reconcile`, `delete`
> and `logs` are routed as their own sub-paths and cannot be addressed as backup
> IDs. Backup IDs are `YYYYMMDDHHMMSS` timestamps, so this never collides in
> practice.

Returns detailed information for a specific backup.

**Response:** `200 OK`

Returns a single `BackupHistory` object (same structure as the items in the list response above).

| Error Code | Condition |
|---|---|
| 400 | Instance ID or Backup ID missing |
| 404 | Instance or backup not found |
| 500 | Internal server error |

---

#### `GET /api/camundas/{id}/backups/{backupId}/logs` — Get Backup Logs

Returns the raw log file contents for a specific backup execution.

**Response:** `200 OK`

**Content-Type:** `text/plain; charset=utf-8`

```
2024-01-15T02:00:00Z [INFO] Starting backup 20240115020000 for prod-cluster
2024-01-15T02:00:01Z [INFO] Backing up component: zeebe
2024-01-15T02:05:23Z [INFO] Component zeebe completed successfully
2024-01-15T02:05:24Z [INFO] Backing up component: operate
...
2024-01-15T02:12:34Z [INFO] Backup 20240115020000 completed successfully
```

| Error Code | Condition |
|---|---|
| 400 | Instance ID or Backup ID missing |
| 404 | Instance, backup, or log file not found |
| 500 | Internal server error |

---

### Retention & Cleanup

#### `DELETE /api/camundas/{id}/backups/{backupId}` — Delete Backup

Permanently deletes a specific backup from every system that holds it:

1. the Elasticsearch snapshot,
2. each Camunda component's backup (Zeebe, Operate, Tasklist, Optimize),
3. the controller's own metadata record in S3,
4. the backup's log file.

This endpoint handles both a **tracked** backup, which the controller has a
metadata record for, and an **orphan**, which it does not. Which path a request
takes is decided by the controller, from whether a record exists — never by the
caller. See [Deleting an orphan](#deleting-an-orphan) below.

The most recent successful backup cannot be deleted, and neither can a backup
that is still `RUNNING` — deleting one races the orchestrator still writing it.
Neither guard is overridable by `force`.

A component that answers `404` counts as already deleted. The backup record's
component map decides what gets purged: the orchestrator seeds it with every
component enabled at backup time, so components absent from the map were
disabled then and are left alone, as are components the record marks skipped.
A record listing no components at all cannot be identified and is refused
(use `force` to drop it anyway).

The metadata record is deleted **last, and only once every artifact is gone**.
If an artifact cannot be deleted the whole delete is refused with `409
artifacts_remain` and the backup stays visible so the operation can be retried —
deleting the record first would strand the artifacts as orphans.

**Query parameters:**

| Parameter | Description |
|---|---|
| `force` | `true` deletes the controller's record even when artifacts survive. The leftovers are then reported by [orphan detection](orphaned-backups.md). |

**Headers:** `X-Requested-With: XMLHttpRequest`

**Response:** `200 OK`

```json
{
  "message": "Backup and all of its artifacts deleted",
  "data": null
}
```

| Error Code | Condition |
|---|---|
| 400 | Instance ID or Backup ID missing |
| 404 | `not_found` — instance not found, or the backup is neither recorded nor reported as an orphan by the latest sweep |
| 404 | `no_report` — the backup has no record and no sweep has run, so nothing can say whether it is an orphan. Repeat DELETEs stay idempotent: an unknown ID always answers 404 |
| 409 | Cannot delete the most recent backup, or the backup is still RUNNING (safety refusal). An **orphan** is also refused while any backup is in flight, because it has no record to read a status from |
| 409 | `artifacts_remain` — one or more artifacts could not be deleted; nothing was removed from the controller. For a **tracked** backup: retry, or repeat with `?force=true`. For an **orphan**: `force` does not apply, so resolve the unreachable source and retry |
| 409 | `stale_report` — the backup was reported as an orphan but has since acquired a record. Re-run the sweep |
| 409 | `report_partial` — the sweep could not reach every source, so what the backup left behind is not fully known. Re-scan once every source is reachable |
| 409 | `report_stale` — the sweep is more than an hour old. Re-scan first |
| 409 | `endpoint_drift` — a component's backup endpoint has changed since the sweep. Re-scan first |
| 409 | `ownership_unverified` — another configured instance holds a record for this ID, or shares the repository or endpoint the artifacts live behind |
| 409 | `unidentifiable` — the sweep found the orphan but named no artifact to delete |
| 409 | `not_deletable` — the orphan's ID is not in the controller's `YYYYMMDDHHMMSS` format, or a snapshot name is not addressable. Remove it by hand |
| 500 | Internal server error |

##### Deleting an orphan

An orphan has no metadata record, so the component map that decides what a
tracked deletion purges does not exist. The **latest reconciliation report**
takes its place: the controller deletes the component backups and the exact
Elasticsearch snapshots that sweep positively found, and nothing else.

Which sweep, and how far it is trusted, is the whole design. Every one of these
is a refusal, not a best effort:

- **The caller cannot name the artifacts.** The report is read server-side. A
  client able to specify what to delete could name any snapshot in the
  repository.
- **A partial sweep authorises nothing.** If any source could not be reached,
  the deletion is refused outright (`report_partial`) rather than removing what
  was seen and stranding the rest. An unreachable source is not evidence of
  absence, and it is not a licence to delete either.
- **An old sweep authorises nothing.** A report older than an hour is refused
  (`report_stale`). A deletion acts on a description of the world, and an old
  description is not one.
- **Endpoints must not have moved.** The sweep records the component endpoints
  it actually talked to; if the instance now points somewhere else the deletion
  is refused (`endpoint_drift`). Backup IDs are timestamps from a cron schedule,
  so the same ID routinely names a live backup in another environment.
- **Ownership must be establishable.** If another configured instance holds a
  record for the same ID, or shares the snapshot repository or a component
  endpoint with nothing to tell their backups apart, the deletion is refused
  (`ownership_unverified`). With the default empty snapshot name prefix, two
  instances share one repository and each sees the other's backups as orphans.
- **No backup may be in flight.** An orphan has no record to read a `RUNNING`
  status from, and a live backup whose initial record write failed looks exactly
  like one, so any in-flight backup refuses the deletion.
- **Only addressable names are deleted.** A component API can report backup IDs
  the controller never generated, and a repository can report snapshot names
  containing path separators or wildcards; both become path segments in the
  `DELETE`. Anything not matching `YYYYMMDDHHMMSS`, or carrying a character
  Elasticsearch itself forbids in a snapshot name, is reported with a
  remediation command and never deleted automatically.

The artifact list also comes from every snapshot the sweep *observed* for that
backup, not only the ones a finding named. Findings are de-duplicated for
reading — a component snapshot is not reported separately while the component
still tracks the backup — and deleting from the reported set alone would leave
those snapshots behind while reporting the backup fully deleted.

`force` has no meaning for an orphan: it exists to drop a controller record
despite surviving artifacts, and an orphan has no record. An orphan whose
artifacts survive answers `409 artifacts_remain`, and the fix is to resolve the
cause and retry.

There is no most-recent-backup guard on this path, and none is needed: the
controller cannot consider a backup it has no record of to be its latest
recovery point. If a record appears between the sweep and the deletion, the
request is refused with `409 stale_report` rather than proceeding — that record's
guards have to be applied, and only the tracked path applies them.

---

#### `POST /api/camundas/{id}/backups/delete` — Delete Several Backups

Deletes a batch of backups, each one through exactly the same path as
`DELETE /api/camundas/{id}/backups/{backupId}` above, tracked or orphaned.

**Headers:** `X-Requested-With: XMLHttpRequest`, `Content-Type: application/json`

**Request body:**

```json
{
  "backup_ids": ["20240115020000", "20240114020000"],
  "force": false
}
```

| Field | Description |
|---|---|
| `backup_ids` | Backup IDs to delete. Duplicates are collapsed; at most 25 per request |
| `force` | Applied to each tracked deletion, exactly as the query parameter above. Ignored for orphans |

**Response:** `200 OK`

A batch is **not a transaction**, and the response does not pretend otherwise.
Deletions span several systems, so one backup can be refused by a safety guard
or blocked by an unreachable component while the rest succeed. The status code
reports only whether the request was valid; the outcome is per backup.

```json
{
  "requested": 3,
  "deleted": ["20240114020000", "20240113020000"],
  "failed": [
    {
      "backup_id": "20240115020000",
      "error": "safety_refusal",
      "message": "cannot delete the most recent successful backup (20240115020000)"
    }
  ]
}
```

Each `failed` entry carries the same `error` code the single-backup endpoint
would have returned for that backup, so a client can react per backup instead of
parsing prose. One extra code is specific to batches: `not_attempted` means the
request ran out of time or was cancelled before reaching that backup, so it was
never touched and can be retried.

The cap is a time budget rather than a taste. Each backup costs an
instance-wide S3 listing plus a fan-out to every component and to Elasticsearch,
and one unreachable component burns four 30s attempts with backoff before giving
up. The batch stops at 90s and reports the remainder as `not_attempted`, leaving
room to write the response inside the server's 120s write timeout — because a
batch that outruns the connection cannot deliver the per-backup outcome that is
the point of the endpoint.

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing, body invalid or carrying trailing content, `backup_ids` empty, more than 25 IDs, or an ID containing a path separator |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `GET /api/camundas/{id}/backups/orphaned` — List Orphaned Backups

Returns backup history records filed under the `orphaned/` prefix in the
controller's bucket.

> **Looking for orphaned backup detection?** That is
> `GET /api/camundas/{id}/backups/reconcile`, documented below. This endpoint
> only lists records already in `orphaned/`; it does not compare the
> controller's metadata against the artifacts that actually exist. See
> [Orphaned Backup Detection](orphaned-backups.md).

**Response:** `200 OK`

```json
[
  {
    "backup_id": "20240110020000",
    "camunda_instance_id": "prod-cluster",
    "camunda_instance_name": "Production Cluster",
    "start_time": "2024-01-10T02:00:00Z",
    "status": "COMPLETED",
    "trigger_type": "SCHEDULED",
    "components": {},
    "backup_stats": {
      "total_components": 0,
      "successful_components": 0,
      "failed_components": 0,
      "skipped_components": 0
    },
    "metadata": {
      "config_version": "",
      "controller_version": "",
      "execution_mode": "",
      "log_file_path": "",
      "backup_reason": ""
    }
  }
]
```

Returns an empty array `[]` if no orphaned backups exist.

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `GET /api/camundas/{id}/backups/reconcile` — Latest Reconciliation Report

Returns the most recent reconciliation sweep: a cross-reference of the
controller's backup records against the artifacts that actually exist in Zeebe,
the Camunda component APIs and the Elasticsearch snapshot repository.

**Response:** `200 OK`

```json
{
  "camunda_instance_id": "prod-cluster",
  "started_at": "2024-01-10T02:05:00Z",
  "finished_at": "2024-01-10T02:05:04Z",
  "snapshot_repository": "camunda-backup",
  "component_endpoints": {
    "zeebe": "https://zeebe.prod.example.com:9600/actuator/backups",
    "operate": "https://operate.prod.example.com/actuator/backups"
  },
  "sources_checked": {
    "controller_s3": { "name": "controller_s3", "reachable": true, "count": 14 },
    "zeebe":         { "name": "zeebe",         "reachable": true, "count": 30 },
    "operate":       { "name": "operate",       "reachable": false, "error": "connection refused", "count": 0 },
    "tasklist":      { "name": "tasklist",      "reachable": false, "skipped": true, "count": 0 },
    "elasticsearch": { "name": "elasticsearch", "reachable": true, "count": 115 }
  },
  "instance_findings": [],
  "backup_issues": [
    {
      "backup_id": "20240110020000",
      "tracked": false,
      "backup_time": "2024-01-10T02:00:00Z",
      "primary_reason": "D2_SPLIT_RESTORE_PAIR",
      "severity": "critical",
      "reasons": [
        {
          "backup_id": "20240110020000",
          "scope": "backup",
          "reason": "D2_SPLIT_RESTORE_PAIR",
          "severity": "critical",
          "present_in": ["zeebe"],
          "missing_in": ["elasticsearch"],
          "snapshot_name": "20240110020000",
          "detail": "present in zeebe; missing from elasticsearch",
          "detected_at": "2024-01-10T02:05:04Z"
        }
      ],
      "implied": ["D1_PARTIAL_SET", "B2_DANGLING_ES_SNAPSHOT"],
      "present_in": ["zeebe"],
      "missing_in": ["elasticsearch"]
    }
  ],
  "repository_findings": []
}
```

**Reading the response:**

- `sources_checked` is not decoration. A source with `reachable: false` and
  `skipped: false` means the sweep could not look there, so **no conclusion was
  drawn from anything missing in it**. A short findings list from a partial
  sweep is not a clean bill of health.
- `skipped: true` means the source is not configured for this instance, which is
  not a failure.
- `tracked` distinguishes the two kinds of problem backup. `false` means the
  controller has no history record for it — a true orphan, which the UI renders
  with status `ORPHANED`. `true` means a known backup that has developed a
  problem; it keeps its real status. Clients that show orphans separately must
  filter on this field.
- `snapshot_repository` and `component_endpoints` carry the values this sweep
  actually resolved, so a client can substitute them into the `{repository}` and
  `{component_backup_endpoint}` placeholders in a reason's `remediation` text.
  Endpoints are returned with any embedded credentials stripped.
- `implied` lists reason codes suppressed because a more specific finding
  already explains them. They are kept as supporting evidence.
- Findings are scoped: `instance_findings` describe the instance as a whole,
  `repository_findings` cover artifacts belonging to no known backup, and
  `backup_issues` are the per-backup rows.

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found (`not_found`), or no sweep has run yet (`no_report`) |
| 500 | Reconciler not configured, or the stored report could not be read |

Both 404s are distinguished by their machine-readable `code`, so clients never
have to string-match the message.

> `404` ("never scanned") and `200` with no findings ("scanned, nothing wrong")
> are different answers. Do not treat them as equivalent.

---

#### `POST /api/camundas/{id}/backups/reconcile` — Run a Sweep

Runs a reconciliation sweep immediately and returns the fresh report, in the
same shape as the `GET` above. Only one sweep runs per instance at a time.

This is **report-only** and never deletes anything.

**Headers:** `X-Requested-With: XMLHttpRequest`

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 403 | Missing `X-Requested-With` header |
| 404 | Instance not found |
| 409 | A sweep is already running for this instance (`sweep_in_progress`) |
| 500 | Reconciler not configured, or the sweep failed |

Capped by `RECONCILE_TIMEOUT_SECONDS` (default 120).

---

#### `GET /api/reconcile/reasons` — Reason Code Catalogue

Returns the description of every reason code. Static and instance independent,
so clients fetch it once and cache it; reports carry only the codes.

**Response:** `200 OK`

```json
[
  {
    "code": "D2_SPLIT_RESTORE_PAIR",
    "scope": "backup",
    "severity": "critical",
    "label": "Zeebe and Elasticsearch halves are split",
    "explanation": "Only one half of the Zeebe plus Elasticsearch pair survives for this backup ID.",
    "impact": "A Camunda restore requires both halves at the same backup ID. This backup is unrestorable, and the surviving half may look healthy on its own.",
    "remediation": "Take a fresh backup immediately, then remove the orphaned half."
  }
]
```

Reason codes are a stable contract and safe to match on from scripts. The full
taxonomy is documented in [Orphaned Backup Detection](orphaned-backups.md).

---

#### `GET /api/camundas/{id}/backups/incomplete` — List Incomplete Backups

Returns backups that were interrupted before all components finished.

**Response:** `200 OK`

```json
[
  {
    "backup_id": "20240113020000",
    "camunda_instance_id": "prod-cluster",
    "camunda_instance_name": "Production Cluster",
    "start_time": "2024-01-13T02:00:00Z",
    "end_time": "2024-01-13T02:03:45Z",
    "duration_seconds": 225,
    "status": "INCOMPLETE",
    "trigger_type": "SCHEDULED",
    "components": {
      "zeebe": {
        "enabled": true,
        "status": "COMPLETED",
        "start_time": "2024-01-13T02:00:01Z",
        "end_time": "2024-01-13T02:03:44Z",
        "duration_seconds": 223
      },
      "operate": {
        "enabled": true,
        "status": "PENDING",
        "duration_seconds": 0
      }
    },
    "backup_stats": {
      "total_components": 4,
      "successful_components": 1,
      "failed_components": 0,
      "skipped_components": 1,
      "running_components": 0,
      "pending_components": 2
    },
    "metadata": {
      "config_version": "1.0",
      "controller_version": "0.5.0",
      "execution_mode": "sequential",
      "log_file_path": "data/logs/prod-cluster/20240113020000.log",
      "backup_reason": "Scheduled backup"
    },
    "error_message": "Backup interrupted: context cancelled"
  }
]
```

Returns an empty array `[]` if no incomplete backups exist.

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 500 | Internal server error |

---

#### `GET /api/camundas/{id}/backups/failed` — List Failed Backups

Returns backups where one or more components failed.

**Response:** `200 OK`

```json
[
  {
    "backup_id": "20240112020000",
    "camunda_instance_id": "prod-cluster",
    "camunda_instance_name": "Production Cluster",
    "start_time": "2024-01-12T02:00:00Z",
    "end_time": "2024-01-12T02:06:15Z",
    "duration_seconds": 375,
    "status": "FAILED",
    "trigger_type": "SCHEDULED",
    "components": {
      "zeebe": {
        "enabled": true,
        "status": "COMPLETED",
        "start_time": "2024-01-12T02:00:01Z",
        "end_time": "2024-01-12T02:04:30Z",
        "duration_seconds": 269
      },
      "operate": {
        "enabled": true,
        "status": "FAILED",
        "start_time": "2024-01-12T02:04:31Z",
        "end_time": "2024-01-12T02:06:15Z",
        "duration_seconds": 104,
        "error_message": "Operate backup API returned 503: service unavailable"
      }
    },
    "backup_stats": {
      "total_components": 4,
      "successful_components": 1,
      "failed_components": 1,
      "skipped_components": 1,
      "running_components": 0,
      "pending_components": 1
    },
    "metadata": {
      "config_version": "1.0",
      "controller_version": "0.5.0",
      "execution_mode": "sequential",
      "log_file_path": "data/logs/prod-cluster/20240112020000.log",
      "backup_reason": "Scheduled backup"
    },
    "error_message": "Backup failed: 1 component(s) failed"
  }
]
```

Returns an empty array `[]` if no failed backups exist.

| Error Code | Condition |
|---|---|
| 400 | Instance ID missing |
| 404 | Instance not found |
| 500 | Internal server error |

---

### Endpoint Check

#### `POST /api/check-endpoint` — Check Endpoint Connectivity

Probes an external endpoint (Camunda, Elasticsearch, or S3) to verify reachability and authentication. Useful for validating configuration before saving.

**Headers:** `X-Requested-With: XMLHttpRequest`

**Request Body:**

```json
{
  "url": "https://es.prod.example.com:9200",
  "type": "elasticsearch",
  "instance_id": "prod-cluster",
  "username": "elastic",
  "password": ""
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `url` | string | **yes** | The endpoint URL to probe |
| `type` | string | no | Probe type: `camunda`, `elasticsearch`, `s3`, or omit for generic |
| `instance_id` | string | no | Instance ID used to resolve stored credentials |
| `username` | string | no | Username for Elasticsearch basic auth |
| `password` | string | no | Password for Elasticsearch basic auth. When omitted, resolved for `instance_id` in order: env var `ELASTICSEARCH_PASSWORD_<INSTANCE_ID>` → UI-stored secret → `DEFAULT_ELASTICSEARCH_PASSWORD` |
| `access_key` | string | no | S3 access key |
| `secret_key` | string | no | S3 secret key. When omitted, resolved for `instance_id` in order: env var `S3_SECRETKEY_<INSTANCE_ID>` → UI-stored secret → `DEFAULT_S3_SECRETKEY` |

**Response:** `200 OK`

The response always returns `200 OK` with a status field indicating the result:

```json
{
  "status": "connected",
  "status_code": 200,
  "message": "Connected and authenticated"
}
```

**Status values:**

| Status | Meaning |
|---|---|
| `connected` | Endpoint is reachable (and authenticated, if credentials were provided) |
| `unauthenticated` | Endpoint is reachable but credentials are missing or invalid |
| `unreachable` | Endpoint cannot be reached (DNS failure, timeout, connection refused, etc.) |

**Example responses by probe type:**

Camunda — connected:
```json
{
  "status": "connected",
  "status_code": 200,
  "message": "Connected successfully"
}
```

Elasticsearch — reachable but unauthenticated:
```json
{
  "status": "unauthenticated",
  "status_code": 401,
  "message": "Reachable but not authenticated"
}
```

S3 — invalid credentials:
```json
{
  "status": "unauthenticated",
  "status_code": 403,
  "message": "Reachable but not authenticated (invalid credentials)"
}
```

Generic — unreachable:
```json
{
  "status": "unreachable",
  "message": "Connection failed: DNS resolution failed"
}
```

| Error Code | Condition |
|---|---|
| 400 | Invalid JSON body, empty URL, or URL targeting a private/loopback address (SSRF protection) |

> **Note:** SSRF protection blocks probes to private, loopback, and link-local IP addresses by default. Set the `PROBE_ALLOW_PRIVATE_IPS=true` environment variable to allow probing internal/self-hosted services.

---

## Data Models

### CamundaInstance

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique identifier (lowercase letters and hyphens) |
| `name` | string | Human-readable display name |
| `base_url` | string | Base URL of the Camunda cluster |
| `enabled` | boolean | Whether scheduled backups are enabled |
| `schedule` | string | Cron expression for backup schedule |
| `success_retention` | integer | Number of successful backups to retain |
| `failure_retention` | integer | Number of failed backups to retain |
| `zeebe_backup_endpoint` | string | Zeebe backup API endpoint |
| `operate_backup_endpoint` | string | Operate backup API endpoint |
| `tasklist_backup_endpoint` | string | Tasklist backup API endpoint |
| `optimize_backup_endpoint` | string | Optimize backup API endpoint |
| `components` | array | Component configurations (see below) |
| `parallel_execution` | boolean | Whether to back up components in parallel |
| `elasticsearch_endpoint` | string | Elasticsearch endpoint URL |
| `elasticsearch_username` | string | Elasticsearch username |
| `s3_endpoint` | string | S3-compatible storage endpoint |
| `s3_accesskey` | string | S3 access key |
| `elasticsearch_password` | string | *(write-only)* ES password to store for this instance. Omit to leave the stored value unchanged; send `""` to clear it. Never returned in responses. |
| `s3_secret_key` | string | *(write-only)* S3 secret key to store for this instance. Omit to leave the stored value unchanged; send `""` to clear it. Never returned in responses. |
| `elasticsearch_password_env_var` | string | *(read-only)* Environment variable name for ES password |
| `s3_secret_key_env_var` | string | *(read-only)* Environment variable name for S3 secret key |
| `elasticsearch_password_set` | boolean | *(read-only)* Whether an ES password is stored for this instance |
| `s3_secret_key_set` | boolean | *(read-only)* Whether an S3 secret key is stored for this instance |
| `created_at` | string (ISO 8601) | Creation timestamp |
| `updated_at` | string (ISO 8601) | Last update timestamp |
| `last_backup_at` | string (ISO 8601) | *(optional)* Timestamp of last backup |
| `last_backup_status` | string | Status of last backup or `NEVER_BACKED_UP` |

### CamundaComponentConfig

| Field | Type | Description |
|---|---|---|
| `name` | string | Component name: `zeebe`, `operate`, `tasklist`, `optimize`, `elasticsearch` |
| `enabled` | boolean | Whether this component is included in backups |

### BackupHistory

| Field | Type | Description |
|---|---|---|
| `backup_id` | string | Unique backup identifier (timestamp-based) |
| `camunda_instance_id` | string | ID of the Camunda instance |
| `camunda_instance_name` | string | Display name of the Camunda instance |
| `start_time` | string (ISO 8601) | When the backup started |
| `end_time` | string (ISO 8601) | *(optional)* When the backup ended |
| `duration_seconds` | integer | *(optional)* Total duration in seconds |
| `status` | string | `RUNNING`, `COMPLETED`, `FAILED`, or `INCOMPLETE` |
| `trigger_type` | string | `SCHEDULED` or `MANUAL` |
| `components` | object | Map of component name → `ComponentBackupInfo` |
| `backup_stats` | object | Aggregate component statistics |
| `metadata` | object | Execution metadata |
| `error_message` | string | *(optional)* Error description if backup failed |

### ComponentBackupInfo

| Field | Type | Description |
|---|---|---|
| `enabled` | boolean | Whether this component was enabled |
| `status` | string | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, or `SKIPPED` |
| `start_time` | string (ISO 8601) | *(optional)* Component backup start time |
| `end_time` | string (ISO 8601) | *(optional)* Component backup end time |
| `duration_seconds` | integer | Duration in seconds |
| `error_message` | string | *(optional)* Error description if component failed |
| `snapshot_name` | string | *(optional)* Elasticsearch snapshot name |
| `snapshot_repository` | string | *(optional)* Elasticsearch snapshot repository |

### BackupStats

| Field | Type | Description |
|---|---|---|
| `total_components` | integer | Total enabled components |
| `successful_components` | integer | Components that completed successfully |
| `failed_components` | integer | Components that failed |
| `skipped_components` | integer | Components that were disabled or skipped |
| `running_components` | integer | *(optional)* Components currently running |
| `pending_components` | integer | *(optional)* Components not yet started |

### BackupMetadata

| Field | Type | Description |
|---|---|---|
| `config_version` | string | Configuration file version |
| `controller_version` | string | Controller application version |
| `execution_mode` | string | `sequential` or `parallel` |
| `log_file_path` | string | Path to the backup log file |
| `backup_reason` | string | Human-readable reason for the backup |

---

## Quick Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe |
| `GET` | `/api/status` | System status |
| `GET` | `/api/camundas` | List all instances |
| `POST` | `/api/camundas` | Create instance |
| `GET` | `/api/camundas/{id}` | Get instance |
| `PUT` | `/api/camundas/{id}` | Update instance |
| `DELETE` | `/api/camundas/{id}` | Delete instance |
| `POST` | `/api/camundas/{id}/enable` | Enable instance |
| `POST` | `/api/camundas/{id}/disable` | Disable instance |
| `POST` | `/api/camundas/{id}/backup` | Trigger manual backup |
| `GET` | `/api/camundas/{id}/backups` | List backup history |
| `GET` | `/api/camundas/{id}/backups/{backupId}` | Get backup details |
| `GET` | `/api/camundas/{id}/backups/{backupId}/logs` | Get backup logs |
| `DELETE` | `/api/camundas/{id}/backups/{backupId}` | Delete one backup, tracked or orphaned |
| `POST` | `/api/camundas/{id}/backups/delete` | Delete several backups in one request |
| `GET` | `/api/camundas/{id}/backups/orphaned` | List records under the `orphaned/` prefix |
| `GET` | `/api/camundas/{id}/backups/reconcile` | Latest reconciliation report |
| `POST` | `/api/camundas/{id}/backups/reconcile` | Run a reconciliation sweep now |
| `GET` | `/api/reconcile/reasons` | Reason code catalogue |
| `GET` | `/api/camundas/{id}/backups/incomplete` | List incomplete backups |
| `GET` | `/api/camundas/{id}/backups/failed` | List failed backups |
| `POST` | `/api/check-endpoint` | Check endpoint connectivity |
