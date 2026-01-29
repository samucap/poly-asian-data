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

	// Initialize logger with default env (will be overridden after config load)
	env := os.Getenv("ENV")
	if env == "" {
		env = "dev"
	}
	logging.Init(env)

	logging.Info("Starting Poly Asian Data Pipeline (Subgraph Sync)...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logging.Error("Failed to load configuration", slog.Any("error", err))
		os.Exit(1)
	}

	// Re-initialize logger with proper environment from config
	logging.Init(cfg.ENV)

	// Log startup info
	logging.Info("Configuration loaded successfully",
		slog.String("environment", cfg.ENV),
		slog.String("log level", cfg.LogLevel),
	)

	logging.Info("Application initialized. Starting subgraph sync pipeline...")

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go subgraphSync(ctx, cfg)

	<-sigChan
	logging.Info("Shutdown signal received. Exiting...")
}

func subgraphSync(ctx context.Context, cfg *config.Config) {
	plyMktPipeline, err := pipeline.New(ctx, cfg)
	if err != nil {
		logging.Error("Failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}
	plyMktPipeline.RunSubgraphSync()
}
