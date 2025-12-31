# Makefile for fis-migration-tool
# Cross-compilation support for multiple platforms

.PHONY: build build-linux-amd64 build-linux-arm64 build-darwin-arm64 build-all clean help

# Binary name
BINARY_NAME := migration

# Build directory
BUILD_DIR := bin

# Version information (optional)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build for current platform
	@echo "Building for current platform..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/migration
	@echo "Binary created: $(BUILD_DIR)/$(BINARY_NAME)"

build-linux-amd64: ## Build for Linux AMD64
	@echo "Building for linux/amd64..."
	@mkdir -p $(BUILD_DIR)/linux-amd64
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o $(BUILD_DIR)/linux-amd64/$(BINARY_NAME) ./cmd/migration
	@echo "Binary created: $(BUILD_DIR)/linux-amd64/$(BINARY_NAME)"

build-linux-arm64: ## Build for Linux ARM64
	@echo "Building for linux/arm64..."
	@mkdir -p $(BUILD_DIR)/linux-arm64
	@GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o $(BUILD_DIR)/linux-arm64/$(BINARY_NAME) ./cmd/migration
	@echo "Binary created: $(BUILD_DIR)/linux-arm64/$(BINARY_NAME)"

build-darwin-arm64: ## Build for macOS ARM64 (Apple Silicon)
	@echo "Building for darwin/arm64..."
	@mkdir -p $(BUILD_DIR)/darwin-arm64
	@GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o $(BUILD_DIR)/darwin-arm64/$(BINARY_NAME) ./cmd/migration
	@echo "Binary created: $(BUILD_DIR)/darwin-arm64/$(BINARY_NAME)"

build-all: build-linux-amd64 build-linux-arm64 build-darwin-arm64 ## Build for all supported platforms
	@echo "All binaries built successfully!"
	@echo "Binaries location:"
	@ls -lh $(BUILD_DIR)/*/$(BINARY_NAME) 2>/dev/null || echo "No binaries found"

clean: ## Remove build directory
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)
	@echo "Build directory removed"

test: ## Run tests
	@echo "Running tests..."
	@go test -v ./...

