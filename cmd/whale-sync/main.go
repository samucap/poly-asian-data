package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger
	env := os.Getenv("ENV")
	if env == "" {
		env = "dev"
	}
	logging.Init(env)

	logging.Info("Starting Poly Asian Whale Sync Pipeline...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logging.Error("Failed to load configuration", slog.Any("error", err))
		os.Exit(1)
	}

	// Re-init logger with config
	logging.Init(cfg.ENV)

	logging.Info("Configuration loaded", slog.String("env", cfg.ENV))

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go runWhaleSync(ctx, cfg)

	<-sigChan
	logging.Info("Shutdown signal received. Exiting...")
	// Can call pipeline.Stop() here if we exposed the instance
}

func runWhaleSync(ctx context.Context, cfg *config.Config) {
	pipe, err := pipeline.New(ctx, logging.Logger, cfg)
	if err != nil {
		logging.Error("Failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}
	pipe.RunWhaleSync(ctx)
}
