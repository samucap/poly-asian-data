# Makefile for Poly Asian Data Pipeline

BINARY_NAME=poly-asian-data
DOCKER_COMPOSE_FILE=docker-compose.yml
DOCKER_COMPOSE_POSTGRES=docker-compose.postgres.yml
DOCKER_COMPOSE_APP=docker-compose.app.yml
MAIN_PATH=cmd/main.go

.PHONY: all build build-catalog build-top-markets build-edge-scan build-edge-eval build-strategy build-ws-listener build-signal-eval clean test coverage lint run run-catalog-markets run-catalog-markets-once run-edge-scan run-edge-scan-once run-edge-eval run-strategy run-strategy-register run-strategy-list run-strategy-active run-strategy-show run-strategy-promote run-strategy-rollback run-strategy-candidate run-ws-listener run-signal-eval test-eval test-strategy test-ws test-signal-eval edge-board-top edge-board-verify edge-scan-once-top edge-scan-top edge-scan-verify dev audit sec docker-up docker-down db-up db-down app-up app-down

all: build

# Pass-through to the Go binary — same flags as `go run ./cmd/... --once`.
# Make steals bare `--once`, so put CLI flags in ARGS:
#   make run-edge-scan ARGS='--once'
#   make run-edge-scan ARGS='--once --weights configs/strategies/default.yaml'
#   make run-catalog-markets ARGS='--once'
# Default ARGS empty = continuous (no --once).
ARGS ?=

# Build the binary
build:
	@echo "Building..."
	CGO_ENABLED=0 go build -ldflags="-w -s" -o bin/$(BINARY_NAME) $(MAIN_PATH)

# Catalog-markets: full open-universe sync + catalog_v1 artifact
build-catalog:
	@echo "Building catalog-markets..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/catalog-markets ./cmd/catalog-markets

build-edge-scan:
	@echo "Building edge-scan..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/edge-scan ./cmd/edge-scan

build-edge-eval:
	@echo "Building edge-eval..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/edge-eval ./cmd/edge-eval

build-strategy:
	@echo "Building strategy (M5)..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/strategy ./cmd/strategy

# Catalog run (default continuous)
run-catalog-markets:
	go run ./cmd/catalog-markets $(ARGS)

run-catalog-markets-once:
	go run ./cmd/catalog-markets --once $(ARGS)

# Wipe tags (FK-safe) + sports-sync + force API catalog rebuild (always one-shot)
run-catalog-markets-reset-tags:
	go run ./cmd/catalog-markets --reset-tags --once $(ARGS)

# Edge-scan run (default continuous)
run-edge-scan:
	go run ./cmd/edge-scan $(ARGS)

run-edge-scan-once:
	go run ./cmd/edge-scan --once $(ARGS)

# M4 edge-eval (DB-only by default; optional --backfill-prices)
#   make run-edge-eval
#   make run-edge-eval ARGS='--lookback 168h --persist-labels'
#   make run-edge-eval ARGS='--backfill-prices --lookback 720h'
#   make run-edge-eval ARGS='--version-id 1'
run-edge-eval:
	go run ./cmd/edge-eval $(ARGS)

# =============================================================================
# M5 strategy versions (register / promote / rollback)
# Make steals bare flags — put them in ARGS / named vars:
#   make run-strategy CMD=list
#   make run-strategy-register
#   make run-strategy-register WEIGHTS=configs/strategies/default.yaml ARGS='--note baseline'
#   make run-strategy-promote ID=1 SURFACE=artifacts/eval_surface/latest.json
#   make run-strategy-active
#   make run-strategy-rollback STRATEGY=default
#   make test-strategy
# =============================================================================
CMD ?=
WEIGHTS ?= configs/strategies/default.yaml
ID ?=
SURFACE ?= artifacts/eval_surface/latest.json
STRATEGY ?= default

run-strategy:
	@if [ -z "$(CMD)" ]; then echo "usage: make run-strategy CMD=<register|list|active|show|promote|rollback|candidate> ARGS='...'"; exit 1; fi
	go run ./cmd/strategy $(CMD) $(ARGS)

run-strategy-register:
	go run ./cmd/strategy register --weights $(WEIGHTS) $(ARGS)

run-strategy-list:
	go run ./cmd/strategy list --strategy $(STRATEGY) $(ARGS)

run-strategy-active:
	go run ./cmd/strategy active --strategy $(STRATEGY) $(ARGS)

run-strategy-show:
	@if [ -z "$(ID)" ]; then echo "usage: make run-strategy-show ID=<version_id>"; exit 1; fi
	go run ./cmd/strategy show --id $(ID) $(ARGS)

run-strategy-promote:
	@if [ -z "$(ID)" ]; then echo "usage: make run-strategy-promote ID=<version_id> SURFACE=$(SURFACE)"; exit 1; fi
	go run ./cmd/strategy promote --id $(ID) --surface $(SURFACE) $(ARGS)

run-strategy-rollback:
	go run ./cmd/strategy rollback --strategy $(STRATEGY) $(ARGS)

run-strategy-candidate:
	@if [ -z "$(ID)" ]; then echo "usage: make run-strategy-candidate ID=<version_id>"; exit 1; fi
	go run ./cmd/strategy candidate --id $(ID) $(ARGS)

test-strategy:
	go test ./internal/strategyreg/ ./internal/db/ -count=1 -run 'Strategy|Promote|Resolve|CheckPromote|Params|Gate'

# =============================================================================
# M6 ws-listener (board-only market WS + paper signals; no OMS)
#   make build-ws-listener
#   make run-ws-listener
#   make run-ws-listener ARGS='--once --no-snapshots'
#   make test-ws
# =============================================================================
build-ws-listener:
	@echo "Building ws-listener (M6)..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/ws-listener ./cmd/ws-listener

run-ws-listener:
	go run ./cmd/ws-listener $(ARGS)

test-ws:
	go test ./internal/ws/market/ ./internal/signals/ ./internal/db/ -count=1 -run 'Cap|Parse|Diff|TakeDirty|Evaluate|BuildFactors|Clean|PriceChange|Book|Subscribe'

# =============================================================================
# M8 signal-eval (paper risk manager + fill metrics for external AO; no OMS)
#   make build-signal-eval
#   make run-signal-eval ARGS='--synthetic'
#   make run-signal-eval ARGS='--lookback 168h --risk configs/risk/default.yaml'
#   make test-signal-eval
# =============================================================================
build-signal-eval:
	@echo "Building signal-eval (M8)..."
	CGO_ENABLED=0 go build -ldflags="-w -s -X github.com/samucap/poly-asian-data/internal/artifacts.CodeCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o bin/signal-eval ./cmd/signal-eval

run-signal-eval:
	go run ./cmd/signal-eval $(ARGS)

test-signal-eval:
	go test ./internal/risk/ ./internal/signaleval/ -count=1

# Explain top N markets from artifacts/edge_board/latest.json (no DB required)
# Usage: make edge-board-top
#        make edge-board-top N=10
#        make edge-board-top BOARD=artifacts/edge_board/<run_id>.json
N ?= 5
BOARD ?= artifacts/edge_board/latest.json
# VERIFY_ARGS e.g. --require-fv --strict-extreme --buffer 20
VERIFY_ARGS ?=
edge-board-top:
	@python3 scripts/edge_board_top.py --path "$(BOARD)" -n $(N)

# M3.5 accuracy/optimality gates on latest artifact (no network)
# Usage: make edge-board-verify
#        make edge-board-verify BOARD=artifacts/edge_board/latest.json VERIFY_ARGS='--require-fv'
edge-board-verify:
	@python3 scripts/edge_board_verify.py --path "$(BOARD)" $(VERIFY_ARGS)

# Run edge-scan with ARGS then print top board
edge-scan-top: run-edge-scan edge-board-top

# One edge-scan cycle then print top board explanation
edge-scan-once-top:
	go run ./cmd/edge-scan --once $(ARGS)
	@$(MAKE) edge-board-top N=$(N) BOARD="$(BOARD)"

# Full M3.5 sign-off: unit tests → one scan → verify → top explain
# Usage: make edge-scan-verify
#        make edge-scan-verify VERIFY_ARGS='--require-fv'
edge-scan-verify:
	@echo "== unit tests (edge + edgescan) =="
	go test ./internal/edge/ ./internal/edgescan/ -count=1
	@echo "== edge-scan --once =="
	go run ./cmd/edge-scan --once $(ARGS)
	@echo "== artifact verify =="
	@python3 scripts/edge_board_verify.py --path "$(BOARD)" $(VERIFY_ARGS)
	@echo "== top board =="
	@python3 scripts/edge_board_top.py --path "$(BOARD)" -n $(N)

# M4.0 + M4 eval package unit tests (vanity win-rate must fail gates)
test-eval:
	go test ./internal/eval/ ./internal/enrich/ ./internal/db/ -count=1 -run 'Eval|Fill|Backtest|Price|Label|SelectPrice|Normalize'

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
