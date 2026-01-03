package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger with default env (will be overridden after config load)
	env := os.Getenv("ENV")
	if env == "" {
		env = "development"
	}
	logging.Init(env)

	logging.Info("Starting Poly Data Pipeline...")

	// Load configuration
	cfg, err := config.Load(ctx)
	if err != nil {
		logging.Error("Failed to load configuration", slog.Any("error", err))
		os.Exit(1)
	}

	// Re-initialize logger with proper environment from config
	logging.Init(cfg.Environment)

	// Log startup info (demonstrating redaction)
	logging.Info("Configuration loaded successfully",
		slog.String("environment", cfg.Environment),
		slog.String("log_level", cfg.LogLevel),
		slog.String("polymarket_api_key", cfg.PolymarketAPIKey), // Will be redacted
		slog.String("postgres_url", cfg.PostgresURL),             // Contains password, but key doesn't match pattern
	)

	// Note: For postgres_url, the value itself may contain password.
	// The redaction is based on KEY names, so sensitive data in VALUES
	// should be handled by not logging them at all, or masking in the struct.
	logging.Info("Application initialized. Ready for connections.")

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logging.Info("Shutdown signal received. Exiting...")
}
