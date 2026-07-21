# Makefile for Poly Asian Data Pipeline

BINARY_NAME=poly-asian-data
DOCKER_COMPOSE_FILE=docker-compose.yml
DOCKER_COMPOSE_POSTGRES=docker-compose.postgres.yml
DOCKER_COMPOSE_APP=docker-compose.app.yml
MAIN_PATH=cmd/main.go

.PHONY: all build build-catalog build-top-markets build-edge-scan clean test coverage lint run run-catalog-once run-edge-scan-once edge-board-top edge-scan-once-top dev audit sec docker-up docker-down db-up db-down app-up app-down

all: build

# Build the binary
build:
	@echo "Building..."
	CGO_ENABLED=0 go build -ldflags="-w -s" -o bin/$(BINARY_NAME) $(MAIN_PATH)

# Catalog-markets: full open-universe sync + catalog_v1 artifact
build-catalog:
	@echo "Building catalog-markets..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/catalog-markets ./cmd/catalog-markets

build-top-markets:
	@echo "Building top-markets..."
	CGO_ENABLED=0 go build -ldflags="-w -s" -o bin/top-markets ./cmd/top-markets

build-edge-scan:
	@echo "Building edge-scan..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/edge-scan ./cmd/edge-scan

# One catalog cycle then exit (needs Postgres + network)
run-catalog-once:
	go run ./cmd/catalog-markets --once

# Wipe tags (FK-safe) + sports-sync + force API catalog rebuild
run-catalog-reset-tags:
	go run ./cmd/catalog-markets --reset-tags --once

run-edge-scan-once:
	go run ./cmd/edge-scan --once

# Explain top N markets from artifacts/edge_board/latest.json (no DB required)
# Usage: make edge-board-top
#        make edge-board-top N=10
#        make edge-board-top BOARD=artifacts/edge_board/<run_id>.json
N ?= 5
BOARD ?= artifacts/edge_board/latest.json
edge-board-top:
	@python3 scripts/edge_board_top.py --path "$(BOARD)" -n $(N)

# One edge-scan cycle then print top board explanation
edge-scan-once-top: run-edge-scan-once edge-board-top

# Clean build artifacts
clean:
	@echo "Cleaning..."
	go clean
	rm -rf bin/
	rm -f coverage.out

# Run tests with gotestsum for colorized output with summary
test:
	@echo "Running tests..."
	@if command -v gotestsum >/dev/null 2>&1; then \
		gotestsum --format pkgname-and-test-fails --no-color=false -- -timeout=60s ./...; \
	else \
		echo "gotestsum not installed. Using go test..."; \
		echo "Install gotestsum: go install gotest.tools/gotestsum@latest"; \
		go test -v -timeout=60s ./...; \
	fi

# Run tests with verbose output (all test names)
test-verbose:
	@echo "Running tests (verbose)..."
	@if command -v gotestsum >/dev/null 2>&1; then \
		gotestsum --format testname -- -timeout=60s ./...; \
	else \
		go test -v -timeout=60s ./...; \
	fi

# Run tests with JSON output (for CI/debugging)
test-json:
	go test -v -json -timeout=60s ./...

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# Run golangci-lint
lint:
	@echo "Linting..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

# Run locally (requires postgres to be running)
run:
	@echo "Running locally..."
	go run $(MAIN_PATH)

# =============================================================================
# Database Commands
# =============================================================================

# Start postgres in background (detached)
db-up:
	@echo "Starting Postgres..."
	docker-compose -f $(DOCKER_COMPOSE_POSTGRES) up -d
	@echo "Postgres is running. Connect via: localhost:5432"

# Stop postgres
db-down:
	@echo "Stopping Postgres..."
	docker-compose -f $(DOCKER_COMPOSE_POSTGRES) down

# View postgres logs
db-logs:
	docker-compose -f $(DOCKER_COMPOSE_POSTGRES) logs -f

# =============================================================================
# App Commands
# =============================================================================

# Start app container (requires postgres to be running)
app-up:
	@echo "Building and starting app..."
	docker-compose -f $(DOCKER_COMPOSE_APP) up --build

# Start app in background
app-up-d:
	@echo "Building and starting app (detached)..."
	docker-compose -f $(DOCKER_COMPOSE_APP) up --build -d

# Stop app container
app-down:
	@echo "Stopping app..."
	docker-compose -f $(DOCKER_COMPOSE_APP) down

# View app logs
app-logs:
	docker-compose -f $(DOCKER_COMPOSE_APP) logs -f

# =============================================================================
# Development Mode
# =============================================================================

# Development mode with hot-reload (requires Air) or falls back to app container
dev:
	@echo "Starting development mode..."
	docker-compose -f $(DOCKER_COMPOSE_APP) up --build; \

# =============================================================================
# Full Stack Commands
# =============================================================================

# Start everything (postgres + app)
docker-up:
	@echo "Starting full Docker environment..."
	docker-compose -f $(DOCKER_COMPOSE_POSTGRES) up -d
	@echo "Waiting for Postgres to be healthy..."
	@sleep 3
	docker-compose -f $(DOCKER_COMPOSE_APP) up --build

# Stop everything
docker-down:
	@echo "Stopping all containers..."
	docker-compose -f $(DOCKER_COMPOSE_APP) down
	docker-compose -f $(DOCKER_COMPOSE_POSTGRES) down

# =============================================================================
# Security Commands
# =============================================================================

# Security audit - Go vulnerability database
audit:
	@echo "Running vulnerability check..."
	@if command -v govulncheck >/dev/null 2>&1; then \
		govulncheck ./...; \
	else \
		echo "govulncheck not installed. Install: go install golang.org/x/vuln/cmd/govulncheck@latest"; \
	fi

# Security scan with gosec
sec:
	@echo "Running security scan..."
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "gosec not installed. Install: go install github.com/securego/gosec/v2/cmd/gosec@latest"; \
	fi