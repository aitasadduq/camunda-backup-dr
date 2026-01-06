.PHONY: build clean run test help

# Binary name
BINARY_NAME=backup-controller
# Build directory
BUILD_DIR=build

help:
	@echo "Available targets:"
	@echo "  build   - Build the application"
	@echo "  clean   - Clean build artifacts"
	@echo "  run     - Build and run the application"
	@echo "  test    - Run tests"
	@echo "  deps    - Download dependencies"

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
	@echo "Running tests..."
	@go test -v ./...

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