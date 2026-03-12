.PHONY: build clean run test test-coverage test-integration test-e2e help test-all deps lint fmt

# Binary name
BINARY_NAME=backup-controller
# Build directory
BUILD_DIR=build
# Coverage output
COVERAGE_DIR=build/coverage
# Packages to test (excludes scripts/ which has pre-existing vet errors)
TEST_PKGS=./internal/... ./pkg/... ./web/...

help:
	@echo "Available targets:"
	@echo "  build            - Build the application"
	@echo "  clean            - Clean build artifacts"
	@echo "  run              - Build and run the application"
	@echo "  test             - Run unit tests"
	@echo "  test-coverage    - Run tests with coverage report"
	@echo "  test-integration - Run integration tests (requires external services)"
	@echo "  test-e2e         - Run end-to-end tests"
	@echo "  test-all         - Run all tests (unit + integration + e2e)"
	@echo "  deps             - Download dependencies"
	@echo "  lint             - Run linters"
	@echo "  fmt              - Format code"

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/server
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete"

run: build
	@echo "Running $(BINARY_NAME)..."
	@$(BUILD_DIR)/$(BINARY_NAME)

test:
	@echo "Running unit tests..."
	@go test $(TEST_PKGS) -count=1

test-coverage:
	@echo "Running tests with coverage..."
	@mkdir -p $(COVERAGE_DIR)
	@go test -coverprofile=$(COVERAGE_DIR)/coverage.out $(TEST_PKGS) -count=1
	@echo ""
	@echo "=== Coverage Summary ==="
	@go tool cover -func=$(COVERAGE_DIR)/coverage.out | tail -1
	@echo ""
	@echo "Per-package coverage:"
	@go tool cover -func=$(COVERAGE_DIR)/coverage.out | grep "total:" || true
	@go test -cover $(TEST_PKGS) -count=1 2>&1 | grep -E 'coverage:|no test'
	@echo ""
	@echo "HTML report: go tool cover -html=$(COVERAGE_DIR)/coverage.out"

test-integration:
	@echo "Running integration tests..."
	@go test -tags=integration $(TEST_PKGS) -count=1 -v

test-e2e:
	@echo "Running end-to-end tests..."
	@go test -tags=e2e ./internal/api/... -count=1 -v

test-all: test test-integration test-e2e

deps:
	@echo "Downloading dependencies..."
	@go mod tidy
	@go mod download
	@echo "Dependencies updated"

lint:
	@echo "Running linters..."
	@golangci-lint run

fmt:
	@echo "Formatting code..."
	@go fmt ./...