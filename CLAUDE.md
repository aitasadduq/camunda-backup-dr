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
- `internal/retention/` — keep-last-N policy, incomplete backup handling, manual deletion
- `internal/reconcile/` — orphaned backup detection: cross-references controller metadata against Zeebe, the component APIs and the ES snapshot repository; report-only
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
- Credentials can also be entered in the UI; they go to `internal/secrets` (`$DATA_DIR/secrets.json`, 0600), never to `config.json`. Resolution order is instance env var > UI-stored secret > global default, wired via `config.SetSecretProvider`
- `web/css/tailwind.css` is a pre-built, purged bundle and there is no Node toolchain here — new markup must reuse utility classes already present in that file, or add rules to the hand-written `web/css/styles.css`
- S3 is the authoritative source of backup existence and restore eligibility; file storage only holds config and logs
- Deleting a backup means deleting it everywhere — ES snapshot, every component backup, the controller's S3 record, the log file. The record goes last and only if every artifact is gone, so a partial failure never silently creates an orphan. This holds on BOTH paths: manual deletion (`retention.Manager.DeleteBackup`, which returns `ErrBackupArtifactsRemain`) and unattended retention (`pruneByStatus`, `pruneFailedBackups`, `cleanupIncompleteBackups`, which keep the record and retry next cycle)
- The backup record's `Components` map is the authority on what a backup wrote. The orchestrator seeds it with every *enabled* component before any of them runs, so a component absent from the map was disabled at backup time and owns no artifact — never purge absent components. A record with no components at all is unidentifiable and is refused rather than deleted
- Tests use table-driven patterns, mock interfaces, `httptest.Server` for external services, and `testEnv` struct with deferred cleanup
- Build tags: `//go:build integration` for ES/S3 tests, `//go:build e2e` for end-to-end tests
- Orphan detection is report-only and never deletes; every conclusion drawn from an artifact being *absent* must be gated on that source having been reachable (see `internal/reconcile/classify.go`)
- Reason codes in `internal/reconcile/reasons.go` are a public API contract — their string values must stay stable, and every code needs a catalogue entry with remediation text
- For the orphan taxonomy and its false-positive guards, see `docs/orphaned-backups.md`
- For architecture details, see `planning/architecture-ait-updated.md`
- For implementation status, see `planning/checklist.md`

## Don't
- Don't store credentials in config JSON or logs — use environment variables, or the `internal/secrets` store for UI-entered values
- Don't allow concurrent backups (scheduled or manual) — the scheduler and orchestrator enforce this via `atomic.Bool`
- Don't delete the most recent successful backup during retention cleanup — retention manager has safety guards for this
- Don't delete a backup's metadata record before its artifacts; that is exactly how orphans are created. A component answering 404 counts as deleted, an unreachable one does not
- Don't delete a backup that is still RUNNING — it races the orchestrator, which would rewrite the record and finish creating artifacts after their deletion. `force` does not override this
- Don't infer a component's participation from the instance's current config; read it from the backup record. Config can change after a backup is taken
- Don't bypass the `AppError` system for HTTP error responses — use `ToHTTPError()` to convert errors to consistent JSON responses
- Don't add an external database — all state is file-based (config/logs on PVC) or in S3 (backup data/history)
- Don't skip middleware ordering — it must be: recovery → logging → CORS → CSRF → content-type
- Don't let the reconciler delete anything, and don't report a backup as missing from a source that could not be enumerated — an unreachable component is not evidence of absence

## gstack
Use the `/browse` skill from gstack for all web browsing. Never use `mcp__claude-in-chrome__*` tools.

Available skills: `/office-hours`, `/plan-ceo-review`, `/plan-eng-review`, `/plan-design-review`, `/design-consultation`, `/review`, `/ship`, `/browse`, `/qa`, `/qa-only`, `/design-review`, `/setup-browser-cookies`, `/retro`, `/investigate`, `/document-release`, `/codex`, `/careful`, `/freeze`, `/guard`, `/unfreeze`, `/gstack-upgrade`
