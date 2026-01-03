# Makefile for Poly Asian Data Pipeline

BINARY_NAME=poly-asian-data
DOCKER_COMPOSE_FILE=docker-compose.yml
MAIN_PATH=cmd/pipeline/main.go

.PHONY: all build clean test coverage lint run dev audit sec docker-up docker-down

all: build

# Build the binary
build:
	@echo "Building..."
	CGO_ENABLED=0 go build -ldflags="-w -s" -o bin/$(BINARY_NAME) $(MAIN_PATH)

# Clean build artifacts
clean:
	@echo "Cleaning..."
	go clean
	rm -rf bin/
	rm -f coverage.out

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

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

# Run locally
run:
	@echo "Running locally..."
	go run $(MAIN_PATH)

# Development mode with hot-reload (requires Air)
dev:
	@echo "Starting development mode..."
	@if command -v air >/dev/null 2>&1; then \
		air; \
	else \
		echo "Air not installed. Install: go install github.com/air-verse/air@latest"; \
		echo "Falling back to docker-compose..."; \
		docker-compose -f $(DOCKER_COMPOSE_FILE) up --build; \
	fi

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

# Docker commands
docker-up:
	@echo "Starting Docker environment..."
	docker-compose -f $(DOCKER_COMPOSE_FILE) up --build

docker-down:
	@echo "Stopping Docker environment..."
	docker-compose -f $(DOCKER_COMPOSE_FILE) down

docker-logs:
	docker-compose -f $(DOCKER_COMPOSE_FILE) logs -f

# Install development tools
tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	go install github.com/air-verse/air@latest
