# ---- Build stage ----
FROM mirror.gcr.io/library/golang:1.23-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Cache dependency downloads
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /backup-controller ./cmd/server

# ---- Runtime stage ----
FROM mirror.gcr.io/library/alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 appuser

COPY --from=builder /backup-controller /usr/local/bin/backup-controller

RUN mkdir -p /data && chown appuser:appuser /data

USER appuser

EXPOSE 8080

VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/healthz"]

ENTRYPOINT ["backup-controller"]
