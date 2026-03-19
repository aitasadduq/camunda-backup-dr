# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project
Camunda Backup & Disaster Recovery Controller — a Go service that manages multi-instance Camunda backups with scheduled/manual triggers, retention policies, S3 storage, and Elasticsearch snapshot integration.

## Stack
- Go 1.23
- Standard library HTTP server (no framework)
- AWS SDK v2 for S3
- Elasticsearch REST API (direct HTTP calls)
- File-based config (JSON) + S3 for backup state
- Deployed via Docker/Kubernetes/Helm

## Structure
- `cmd/server/` — entry point, wires all components, graceful shutdown (30s timeout)
- `internal/api/` — HTTP handlers, routes, middleware stack (recovery → logging → CORS → CSRF → content-type), embedded web UI
- `internal/orchestrator/` — backup workflow orchestration (parallel/sequential modes)
- `internal/scheduler/` — cron-based scheduling with concurrent backup prevention
- `internal/camunda/` — Camunda instance management and HTTP client with retry/backoff
- `internal/storage/` — file storage (config/logs) and S3 storage (backup data) behind interfaces
- `internal/elasticsearch/` — ES snapshot creation and status checking
- `internal/retention/` — keep-last-N policy, orphaned/incomplete backup handling
- `internal/utils/` — structured logging, `AppError` type, circuit breaker, alerting
- `internal/models/` — `CamundaInstance`, `BackupExecution`, `BackupHistory`
- `internal/config/` — env-var-driven configuration with defaults and validation
- `pkg/types/` — shared constants (`BackupStatus`, `ComponentStatus`, `TriggerType`, component names)
- `web/` — embedded HTML/CSS/JS dashboard
- `deployments/` — Dockerfile, docker-compose, k8s manifests, Helm chart
- `docs/` — API, deployment, error-handling, troubleshooting guides
- `planning/` — architecture docs and implementation checklist

## Commands
- Dev: `docker compose -f deployments/docker-compose.yaml up`
- Build: `make build` (output: `build/backup-controller`)
- Run: `make run`
- Test: `make test`
- Test single: `go test ./internal/api/... -run TestHandlerName -v`
- Test integration: `make test-integration` (requires ES/S3 running)
- Test e2e: `make test-e2e`
- Test all: `make test-all`
- Lint: `make lint`
- Format: `make fmt`
- Deps: `make deps`

## Verification
After every change, run in this order:
1. `make build` — fix compile errors
2. `make test` — fix failing tests
3. `make lint` — fix lint errors (if golangci-lint installed)

## Conventions
- Interface-based design: all external dependencies (FileStorage, S3Storage, CamundaManager) use interfaces for testability
- Custom `AppError` type with error codes, HTTP status mapping, and `Unwrap()` chaining — see `internal/utils/apperror.go`
- Structured contextual loggers: `BackupLogger` (with backup ID), `ContextLogger` (with operation/component/instance) — see `internal/utils/logger.go`
- Adapter pattern in `main.go`: `backupExecutorAdapter` bridges orchestrator to scheduler
- Per-instance credential overrides via env vars: `ELASTICSEARCH_PASSWORD_<INSTANCE_ID>`, `S3_SECRETKEY_<INSTANCE_ID>` (hyphens → underscores, uppercased)
- S3 is the authoritative source of backup existence and restore eligibility; file storage only holds config and logs
- Tests use table-driven patterns, mock interfaces, `httptest.Server` for external services, and `testEnv` struct with deferred cleanup
- Build tags: `//go:build integration` for ES/S3 tests, `//go:build e2e` for end-to-end tests
- For architecture details, see `planning/architecture-ait-updated.md`
- For implementation status, see `planning/checklist.md`

## Don't
- Don't store credentials in file storage or config JSON — use environment variables only
- Don't allow concurrent backups (scheduled or manual) — the scheduler and orchestrator enforce this via `atomic.Bool`
- Don't delete the most recent successful backup during retention cleanup — retention manager has safety guards for this
- Don't bypass the `AppError` system for HTTP error responses — use `ToHTTPError()` to convert errors to consistent JSON responses
- Don't add an external database — all state is file-based (config/logs on PVC) or in S3 (backup data/history)
- Don't skip middleware ordering — it must be: recovery → logging → CORS → CSRF → content-type
