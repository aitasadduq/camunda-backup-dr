# Camunda Backup Controller

A multi-instance Camunda backup and restore controller service.

## Overview

This project provides a robust backup controller service for managing backups of multiple Camunda instances. It supports scheduled and manual backups, retention policies, and integration with S3-compatible object storage and Elasticsearch.

## Architecture

The project follows a layered architecture with clear separation of concerns:

- **HTTP API Layer**: REST endpoints and Web UI for management
- **Business Logic Layer**: Backup orchestration, Camunda management, scheduling
- **Storage Layer**: File storage for configuration and logs, S3 for backup data
- **External Integrations**: Camunda instances, Elasticsearch, Object Storage

For detailed architecture documentation, see [docs/architecture.md](docs/architecture.md).

## Project Structure

```
backup-controller/
├── cmd/
│   └── server/          # Application entry point
├── internal/            # Private application code
│   ├── api/            # HTTP API handlers
│   ├── backup/         # Backup orchestration
│   ├── camunda/        # Camunda instance management
│   ├── config/         # Configuration loading
│   ├── elasticsearch/  # Elasticsearch integration
│   ├── models/         # Data models
│   ├── retention/      # Retention policy management
│   ├── scheduler/      # Cron-based scheduler
│   ├── storage/        # File and S3 storage
│   └── utils/          # Logging and error handling
├── pkg/
│   └── types/          # Shared types and constants
├── web/                # Static web UI assets
├── configs/            # Configuration examples
├── deployments/        # Deployment manifests
├── scripts/            # Build and test scripts
└── docs/               # Documentation
```

## Current Status: Phase 1 - Project Setup and Core Infrastructure ✓

### Completed Features (Phase 1)

- [x] Go module initialization
- [x] Project folder structure creation
- [x] Basic logging infrastructure with structured logging
- [x] Configuration loading system with environment variables
- [x] Default configuration values implementation
- [x] Custom error types and error handling
- [x] Core data models (CamundaInstance, BackupExecution, BackupHistory)
- [x] Shared types and constants (BackupStatus, ComponentStatus, TriggerType)
- [x] Application entry point with graceful shutdown
- [x] Build system (Makefile and build scripts)
- [x] Configuration validation
- [x] Quality gate passed: Project builds successfully

### Quality Gate Status

- ✅ Project builds successfully with `go build`
- ✅ Configuration loads from environment variables
- ✅ Basic logging works and outputs to stdout/stderr
- ✅ Configuration validation catches invalid inputs
- ✅ Graceful shutdown implemented

## Getting Started

### Prerequisites

- Go 1.19 or higher
- Access to S3-compatible object storage (for Phase 2+)
- Access to Camunda instances (for Phase 3+)
- Access to Elasticsearch (for Phase 5+)

### Building

```bash
# Clone the repository
git clone https://github.com/aitasadduq/camunda-backup-dr.git
cd camunda-backup-dr

# Build the application
go build -o build/backup-controller ./cmd/server

# Or use the build script
./scripts/build.sh

# Or use Make (if available)
make build
```

### Running

```bash
# Run with default settings
./build/backup-controller

# Run with custom configuration
PORT=8080 \
LOG_LEVEL=info \
DATA_DIR=/data \
./build/backup-controller
```

### Configuration

The application is configured via environment variables:

#### Service Configuration
- `PORT` - HTTP server port (default: 8080)
- `LOG_LEVEL` - Logging level: debug, info, warn, error (default: info)
- `DATA_DIR` - Data directory for file storage (default: /data)

#### Default Configuration
- `DEFAULT_SCHEDULE` - Default cron schedule for backups (default: "0 2 * * *")
- `DEFAULT_RETENTION_COUNT` - Default number of backups to keep (default: 7)
- `DEFAULT_SUCCESS_HISTORY` - Default success history count (default: 30)
- `DEFAULT_FAILURE_HISTORY` - Default failure history count (default: 30)

#### Default Elasticsearch
- `DEFAULT_ELASTICSEARCH_ENDPOINT` - Default Elasticsearch endpoint (default: "")
- `DEFAULT_ELASTICSEARCH_USERNAME` - Default Elasticsearch username (default: "")

#### Default S3
- `DEFAULT_S3_ENDPOINT` - Default S3 endpoint (default: "")
- `DEFAULT_S3_ACCESSKEY` - Default S3 access key (default: "")

#### Per-Instance Credentials (Future)
- `ELASTICSEARCH_PASSWORD_<CAMUNDA_ID>` - Elasticsearch password for specific instance
- `S3_SECRETKEY_<CAMUNDA_ID>` - S3 secret key for specific instance

### Development

```bash
# Run tests
go test ./...

# Format code
go fmt ./...

# Clean build artifacts
rm -rf build/
```

## Implementation Phases

This project is being implemented in phases:

1. **Phase 1**: Project Setup and Core Infrastructure ✓ (COMPLETED)
2. **Phase 2**: Data Models and Storage Layer (NEXT)
3. **Phase 3**: Camunda Instance Management
4. **Phase 4**: Backup Orchestrator
5. **Phase 5**: Elasticsearch Integration
6. **Phase 6**: S3 Integration
7. **Phase 7**: Scheduler Service
8. **Phase 8**: HTTP API Layer
9. **Phase 9**: Retention Manager
10. **Phase 10**: Web UI
11. **Phase 11**: Error Handling and Resilience
12. **Phase 12**: Testing and Documentation
13. **Phase 13**: Deployment

For detailed implementation checklist, see [checklistv2.md](checklistv2.md).

## License

See [LICENSE](LICENSE) file for details.

## Contributing

This is an internal project for the Camunda Backup & Disaster Recovery solution.

## Support

For issues and questions, please refer to the project documentation or contact the development team.