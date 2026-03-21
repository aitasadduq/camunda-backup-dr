# Deployment Guide

This guide covers building, configuring, and deploying the Camunda Backup Controller in local, Docker, and Kubernetes environments.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Building](#building)
- [Running Locally](#running-locally)
- [Environment Variables](#environment-variables)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Helm Chart](#helm-chart)
- [Health Checks](#health-checks)
- [Security Considerations](#security-considerations)
- [Backup Data Persistence](#backup-data-persistence)
- [Scaling Notes](#scaling-notes)

---

## Prerequisites

| Requirement | Minimum Version | Notes |
|-------------|----------------|-------|
| **Go** | 1.23+ | Required by AWS SDK v2 dependencies |
| **S3-compatible storage** | — | MinIO, AWS S3, or any S3-compatible provider |
| **Elasticsearch** | 8.x | With snapshot repository configured |
| **Camunda instances** | 8.x | Zeebe, Operate, Tasklist (Optimize optional) |

### External services the controller connects to

- **Camunda component APIs** — Zeebe, Operate, Tasklist, and Optimize backup/status endpoints.
- **Elasticsearch** — Snapshot API for Elasticsearch index backups.
- **S3-compatible object storage** — Stores backup data, metadata, and backup state.

---

## Building

### Using `go build`

```bash
go build -o build/backup-controller ./cmd/server
```

### Using Make

```bash
# Build the binary
make build

# Build and run
make run

# Download/tidy dependencies
make deps

# Run tests
make test

# Clean build artifacts
make clean
```

The compiled binary is placed at `build/backup-controller`.

### Cross-compilation

Go supports cross-compilation natively. Set `GOOS` and `GOARCH` before building:

```bash
# Linux amd64 (typical for Docker/Kubernetes)
GOOS=linux GOARCH=amd64 go build -o build/backup-controller-linux-amd64 ./cmd/server

# Linux arm64
GOOS=linux GOARCH=arm64 go build -o build/backup-controller-linux-arm64 ./cmd/server

# macOS arm64 (Apple Silicon)
GOOS=darwin GOARCH=arm64 go build -o build/backup-controller-darwin-arm64 ./cmd/server
```

### Build with version info (optional)

```bash
go build -ldflags "-X main.version=$(git describe --tags --always)" \
  -o build/backup-controller ./cmd/server
```

---

## Running Locally

### Minimal startup

```bash
# Create a local data directory
mkdir -p /tmp/camunda-backup-data

# Run with minimal configuration
DATA_DIR=/tmp/camunda-backup-data \
LOG_LEVEL=debug \
PORT=8080 \
./build/backup-controller
```

The controller starts an HTTP server on the configured port (default `8080`) with a web UI and REST API. Without S3 credentials, it falls back to a mock S3 storage for development.

### Full local configuration

```bash
DATA_DIR=/tmp/camunda-backup-data \
PORT=8080 \
LOG_LEVEL=info \
DEFAULT_SCHEDULE="0 2 * * *" \
DEFAULT_RETENTION_COUNT=7 \
DEFAULT_S3_ENDPOINT="http://localhost:9000" \
DEFAULT_S3_ACCESSKEY="minioadmin" \
DEFAULT_S3_SECRETKEY="minioadmin" \
DEFAULT_ELASTICSEARCH_ENDPOINT="http://localhost:9200" \
DEFAULT_ELASTICSEARCH_USERNAME="elastic" \
ELASTICSEARCH_PASSWORD_MY_CLUSTER="changeme" \
S3_SECRETKEY_MY_CLUSTER="minioadmin" \
./build/backup-controller
```

Once running:

- **Web UI**: `http://localhost:8080/`
- **API**: `http://localhost:8080/api/`
- **Health**: `http://localhost:8080/healthz`
- **Readiness**: `http://localhost:8080/readyz`

---

## Environment Variables

### Service Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `PORT` | int | `8080` | HTTP server port |
| `LOG_LEVEL` | string | `info` | Log verbosity: `debug`, `info`, `warn`, `error` |
| `DATA_DIR` | string | `/data` | Directory for local file storage (config, logs) |

### Default Backup Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DEFAULT_SCHEDULE` | string | `0 2 * * *` | Default cron schedule for backups (daily 2:00 AM) |
| `DEFAULT_RETENTION_COUNT` | int | `7` | Number of successful backups to retain per instance |
| `DEFAULT_SUCCESS_HISTORY` | int | `30` | Days to keep success history records |
| `DEFAULT_FAILURE_HISTORY` | int | `30` | Days to keep failure history records |

### Backup Polling

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DEFAULT_BACKUP_POLL_INTERVAL` | int | `5` | Seconds between backup status polls |
| `DEFAULT_BACKUP_MAX_ATTEMPTS` | int | `120` | Maximum polling attempts before timeout |

### Default Elasticsearch Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DEFAULT_ELASTICSEARCH_ENDPOINT` | string | _(empty)_ | Elasticsearch URL (e.g. `http://es:9200`). Pre-populates new instance forms. |
| `DEFAULT_ELASTICSEARCH_USERNAME` | string | _(empty)_ | Elasticsearch username. Pre-populates new instance forms. |
| `DEFAULT_ELASTICSEARCH_PASSWORD` | string | _(empty)_ | Global fallback Elasticsearch password. Used when no instance-specific `ELASTICSEARCH_PASSWORD_<ID>` is set. |
| `DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY` | string | `camunda-backup` | Snapshot repository name |
| `DEFAULT_ELASTICSEARCH_SNAPSHOT_NAME_PREFIX` | string | _(empty)_ | Prefix for snapshot names |

### Default S3 Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DEFAULT_S3_ENDPOINT` | string | _(required)_ | S3-compatible endpoint URL (e.g. `https://s3.amazonaws.com` or `http://minio:9000`). |
| `DEFAULT_S3_ACCESSKEY` | string | _(required)_ | S3 access key ID. |
| `DEFAULT_S3_SECRETKEY` | string | _(required)_ | S3 secret access key. Falls back per-instance via `S3_SECRETKEY_<ID>`. |
| `DEFAULT_S3_BUCKET` | string | `camunda-backups` | S3 bucket name for storing backup history and IDs. |
| `DEFAULT_S3_REGION` | string | `us-east-1` | AWS region for the S3 bucket. |
| `DEFAULT_S3_PREFIX` | string | _(empty)_ | Key prefix inside the bucket (e.g. `prod/backups`). |
| `DEFAULT_S3_USE_PATH_STYLE` | string | `true` | Use path-style addressing. Required for MinIO; set `false` for AWS S3. |

### Alert and Resilience Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `ALERT_WEBHOOK_URL` | string | _(empty, disabled)_ | Webhook URL for alert notifications (e.g. Slack incoming webhook) |
| `BACKUP_STUCK_TIMEOUT_MINUTES` | int | `120` | Minutes before a running backup is considered stuck. `0` disables detection. |

### Endpoint Security Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `PROBE_INSECURE_SKIP_VERIFY` | bool | `false` | Skip TLS certificate verification for endpoint probes |
| `PROBE_ALLOW_PRIVATE_IPS` | bool | `false` | Allow probes to private/loopback IPs (disables SSRF protection) |

> **⚠️ Warning:** `PROBE_INSECURE_SKIP_VERIFY` and `PROBE_ALLOW_PRIVATE_IPS` weaken security. Use only in development or trusted networks.

### Per-Instance Credentials

Credentials are set per Camunda instance using a naming convention. The instance ID is normalized to **uppercase** with **hyphens replaced by underscores** (e.g. `prod-cluster` → `PROD_CLUSTER`).

| Variable Pattern | Description |
|-----------------|-------------|
| `ELASTICSEARCH_PASSWORD_<ID>` | Elasticsearch password for the instance (falls back to `DEFAULT_ELASTICSEARCH_PASSWORD`) |
| `ELASTICSEARCH_SNAPSHOT_REPOSITORY_<ID>` | Snapshot repository override (falls back to default) |
| `ELASTICSEARCH_SNAPSHOT_NAME_PREFIX_<ID>` | Snapshot name prefix override (falls back to default) |
| `S3_SECRETKEY_<ID>` | S3 secret key for the instance (falls back to `DEFAULT_S3_SECRETKEY`) |

**Example** for an instance named `prod-cluster`:

```bash
export ELASTICSEARCH_PASSWORD_PROD_CLUSTER="secret123"
export ELASTICSEARCH_SNAPSHOT_REPOSITORY_PROD_CLUSTER="prod-snapshots"
export S3_SECRETKEY_PROD_CLUSTER="s3secret456"
```

---

## Docker Deployment

### Dockerfile

```dockerfile
# Build stage
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /backup-controller ./cmd/server

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 appuser

COPY --from=builder /backup-controller /usr/local/bin/backup-controller

RUN mkdir -p /data && chown appuser:appuser /data
USER appuser

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["backup-controller"]
```

### Building the image

```bash
docker build -t camunda-backup-controller:latest .
```

### Running the container

```bash
docker run -d \
  --name camunda-backup-controller \
  -p 8080:8080 \
  -v camunda-backup-data:/data \
  -e LOG_LEVEL=info \
  -e DEFAULT_S3_ENDPOINT=http://minio:9000 \
  -e DEFAULT_S3_ACCESSKEY=minioadmin \
  -e DEFAULT_S3_SECRETKEY=minioadmin \
  -e DEFAULT_ELASTICSEARCH_ENDPOINT=http://elasticsearch:9200 \
  -e DEFAULT_ELASTICSEARCH_USERNAME=elastic \
  -e ELASTICSEARCH_PASSWORD_MY_CLUSTER=changeme \
  -e S3_SECRETKEY_MY_CLUSTER=minioadmin \
  camunda-backup-controller:latest
```

### Docker Compose (development)

```yaml
services:
  backup-controller:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - backup-data:/data
    environment:
      LOG_LEVEL: debug
      DEFAULT_S3_ENDPOINT: http://minio:9000
      DEFAULT_S3_ACCESSKEY: minioadmin
      DEFAULT_S3_SECRETKEY: minioadmin
      DEFAULT_ELASTICSEARCH_ENDPOINT: http://elasticsearch:9200
      DEFAULT_ELASTICSEARCH_USERNAME: elastic
    depends_on:
      - minio
      - elasticsearch

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    ports:
      - "9200:9200"
    environment:
      discovery.type: single-node
      ELASTIC_PASSWORD: changeme
      xpack.security.enabled: "true"

volumes:
  backup-data:
```

---

## Kubernetes Deployment

### Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: camunda-backup
```

### ConfigMap — Default settings

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: backup-controller-config
  namespace: camunda-backup
data:
  PORT: "8080"
  LOG_LEVEL: "info"
  DATA_DIR: "/data"
  DEFAULT_SCHEDULE: "0 2 * * *"
  DEFAULT_RETENTION_COUNT: "7"
  DEFAULT_SUCCESS_HISTORY: "30"
  DEFAULT_FAILURE_HISTORY: "30"
  DEFAULT_BACKUP_POLL_INTERVAL: "5"
  DEFAULT_BACKUP_MAX_ATTEMPTS: "120"
  DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY: "camunda-backup"
  BACKUP_STUCK_TIMEOUT_MINUTES: "120"
```

### Secret — Credentials

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: backup-controller-secrets
  namespace: camunda-backup
type: Opaque
stringData:
  DEFAULT_S3_ENDPOINT: "https://s3.example.com"
  DEFAULT_S3_ACCESSKEY: "AKIAIOSFODNN7EXAMPLE"
  DEFAULT_S3_SECRETKEY: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  DEFAULT_ELASTICSEARCH_ENDPOINT: "https://elasticsearch.example.com:9200"
  DEFAULT_ELASTICSEARCH_USERNAME: "elastic"
  # Per-instance credentials
  ELASTICSEARCH_PASSWORD_PROD_CLUSTER: "prod-es-password"
  S3_SECRETKEY_PROD_CLUSTER: "prod-s3-secret"
  # Alert webhook
  ALERT_WEBHOOK_URL: "https://hooks.slack.com/services/T00/B00/xxx"
```

### PersistentVolumeClaim — Data persistence

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: backup-controller-data
  namespace: camunda-backup
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: standard
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backup-controller
  namespace: camunda-backup
  labels:
    app: backup-controller
spec:
  replicas: 1  # Must be 1 — see Scaling Notes
  strategy:
    type: Recreate  # Avoid two pods writing to the same PVC
  selector:
    matchLabels:
      app: backup-controller
  template:
    metadata:
      labels:
        app: backup-controller
    spec:
      securityContext:
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
      containers:
        - name: backup-controller
          image: camunda-backup-controller:latest
          ports:
            - name: http
              containerPort: 8080
              protocol: TCP
          envFrom:
            - configMapRef:
                name: backup-controller-config
            - secretRef:
                name: backup-controller-secrets
          volumeMounts:
            - name: data
              mountPath: /data
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 5
            periodSeconds: 15
            timeoutSeconds: 5
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
            initialDelaySeconds: 10
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 3
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 256Mi
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: backup-controller-data
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: backup-controller
  namespace: camunda-backup
  labels:
    app: backup-controller
spec:
  type: ClusterIP
  ports:
    - name: http
      port: 8080
      targetPort: http
      protocol: TCP
  selector:
    app: backup-controller
```

### Resource Recommendations

| Resource | Request | Limit | Notes |
|----------|---------|-------|-------|
| CPU | 100m | 500m | Mostly idle; spikes during concurrent backup polling |
| Memory | 128Mi | 256Mi | Low baseline; increase if managing many instances |
| PVC | 1Gi | — | Stores config files and backup logs only (backup data goes to S3) |

Adjust based on the number of Camunda instances managed and backup frequency. The controller itself is lightweight — CPU and memory usage scales with the number of concurrent backup operations, not with backup data size.

### Applying manifests

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f pvc.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```

Or combine them into a single file separated by `---` and apply at once:

```bash
kubectl apply -f deploy/k8s/
```

---

## Helm Chart

> **Note:** A Helm chart has not been created yet — this is planned for future work.

A Helm chart for this project would follow this structure:

```
charts/backup-controller/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── pvc.yaml
│   └── NOTES.txt
└── README.md
```

### Anticipated `values.yaml` sections

- `replicaCount` — Fixed at `1` (see [Scaling Notes](#scaling-notes)).
- `image.repository`, `image.tag` — Container image configuration.
- `config.*` — Maps to ConfigMap env vars (port, log level, schedules, retention).
- `secrets.*` — Maps to Secret env vars (S3 credentials, ES passwords, webhook URL).
- `persistence.enabled`, `persistence.size`, `persistence.storageClass` — PVC settings.
- `resources.requests`, `resources.limits` — Pod resource configuration.
- `probes.liveness`, `probes.readiness` — Health check tuning.

---

## Health Checks

The controller exposes two HTTP endpoints for Kubernetes probes:

### `GET /healthz` — Liveness Probe

Returns `200 OK` if the process is alive. This is a lightweight check — if the HTTP server can respond, the service is healthy.

```json
{
  "status": "healthy",
  "checks": {
    "service": "ok"
  },
  "timestamp": "2024-01-15T14:30:52Z"
}
```

**Kubernetes configuration:**

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 3
```

### `GET /readyz` — Readiness Probe

Returns `200 OK` when the scheduler is running and the Camunda manager is accessible. Returns `503 Service Unavailable` if any subsystem is not ready.

**Healthy response (`200`):**

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

**Not ready response (`503`):**

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

**Kubernetes configuration:**

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### Recommended probe tuning

| Parameter | Liveness | Readiness | Rationale |
|-----------|----------|-----------|-----------|
| `initialDelaySeconds` | 5 | 10 | Readiness needs scheduler startup time |
| `periodSeconds` | 15 | 10 | Readiness should recover traffic quickly |
| `timeoutSeconds` | 5 | 5 | Both endpoints are fast |
| `failureThreshold` | 3 | 3 | Tolerate brief transient failures |

---

## Security Considerations

### No built-in authentication

The Camunda Backup Controller does **not** include built-in authentication or authorization. This is by design — it is intended to run in a trusted network environment with external access control.

**Recommendations:**

- Deploy behind an ingress controller or API gateway that enforces authentication (e.g. OAuth2 Proxy, Nginx with basic auth).
- Use Kubernetes NetworkPolicy to restrict which pods can reach the controller.
- Use Kubernetes RBAC to control who can manage the controller's namespace and resources.
- Do **not** expose the service directly to the internet.

### Credential management

All sensitive values (passwords, secret keys, webhook URLs) are passed via environment variables:

- **Never** store credentials in the application's data directory or configuration files.
- Use Kubernetes Secrets for credential injection.
- Consider integrating with an external secret manager (e.g. HashiCorp Vault, AWS Secrets Manager) via tools like [External Secrets Operator](https://external-secrets.io/).
- Rotate credentials by updating the Kubernetes Secret and restarting the pod.

### Endpoint probe security

The controller validates Camunda and Elasticsearch endpoint URLs when instances are created. By default:

- TLS certificates are verified (`PROBE_INSECURE_SKIP_VERIFY=false`).
- Connections to private/loopback IPs are blocked to prevent SSRF (`PROBE_ALLOW_PRIVATE_IPS=false`).

Only relax these settings in development or when running inside a trusted network where the Camunda instances use self-signed certificates or private addresses.

### Network-level security example (Kubernetes)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: backup-controller-network-policy
  namespace: camunda-backup
spec:
  podSelector:
    matchLabels:
      app: backup-controller
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring  # Allow Prometheus scraping
        - podSelector:
            matchLabels:
              app: ingress-nginx  # Allow ingress traffic
      ports:
        - port: 8080
          protocol: TCP
  egress:
    - to: []  # Allow all egress (S3, ES, Camunda endpoints)
```

---

## Backup Data Persistence

### What is stored locally

The controller's `DATA_DIR` (`/data` by default) contains:

- **Camunda instance configuration** — instance definitions, endpoint URLs, component settings, schedules.
- **Backup execution logs** — per-backup log output for troubleshooting.

This data is critical for the controller to resume operations after a restart. **It must be persisted using a PersistentVolumeClaim (PVC).**

### What is stored in S3

Actual backup data, backup metadata, and backup state are stored in S3. The controller uses S3 as the authoritative source for:

- Backup existence and completion status.
- Backup history and retention eligibility.

### PVC requirements

| Setting | Recommendation | Reason |
|---------|---------------|--------|
| Access mode | `ReadWriteOnce` | Single pod access only |
| Storage size | 1Gi | Sufficient for config and logs; backup data goes to S3 |
| Storage class | Platform default or SSD | Low IOPS requirements; any storage class works |

If the PVC is lost, the controller loses its instance configuration and must be reconfigured. Backup data in S3 is unaffected.

### Deployment strategy

Use `strategy.type: Recreate` in the Deployment to ensure the old pod is terminated before the new one starts. This prevents two pods from writing to the same PVC simultaneously.

```yaml
spec:
  strategy:
    type: Recreate
```

---

## Scaling Notes

### Single instance only — no high availability

The Camunda Backup Controller **must run as a single replica**. Do not set `replicas` greater than `1`.

### Why no horizontal scaling

1. **File-based state** — Instance configuration and logs are stored on a local filesystem (PVC). Multiple replicas would need shared storage with locking, which adds complexity without benefit for this workload.

2. **Scheduler coordination** — The built-in cron scheduler runs in-process. Multiple replicas would trigger duplicate backups for the same Camunda instance on the same schedule.

3. **No concurrent backups per instance** — The orchestrator enforces that only one backup can run at a time for a given Camunda instance. Multiple replicas could race and violate this constraint.

4. **Backup-as-control-plane** — The controller is a control plane that orchestrates backups via external APIs (Camunda, Elasticsearch, S3). It does not process backup data itself, so it does not benefit from horizontal scaling.

### Availability strategy

Instead of running multiple replicas, rely on Kubernetes to ensure availability:

- **Deployment with `replicas: 1`** — Kubernetes restarts the pod if it crashes.
- **Liveness probe** — Kubernetes restarts the pod if the health check fails.
- **Readiness probe** — Kubernetes stops routing traffic until the pod is ready.
- **PVC with `Recreate` strategy** — Ensures clean handoff between old and new pods during updates.

If the pod restarts, in-progress backups may be interrupted. The stuck backup detection mechanism (`BACKUP_STUCK_TIMEOUT_MINUTES`) alerts operators when a backup is not progressing, and the scheduler resumes normal operations on the next cron tick.
