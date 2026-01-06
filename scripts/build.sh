#!/bin/bash

# Build script for Camunda Backup Controller

set -e

echo "Building Camunda Backup Controller..."

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Change to project root
cd "$PROJECT_ROOT"

# Create build directory
mkdir -p build

# Build the application
echo "Building binary..."
go build -o build/backup-controller ./cmd/server

echo "Build complete: build/backup-controller"