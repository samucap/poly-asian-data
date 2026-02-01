package main

import (
	"context"

	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
)

func main() {
	// Load .env
	if err := godotenv.Load(); err != nil {
		slog.Warn("No .env file found")
	}



	// Initialize logger with default env
	logging.Init("dev")
	logger := logging.Logger

	// Config
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	// Context with interrupt handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pipeline
	pipe, err := pipeline.New(ctx, logger, cfg)
	if err != nil {
		logger.Error("failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("received signal, stopping pipeline...")
		pipe.Stop()
		cancel()
	}()

	// Run Top Sync
	pipe.RunTopSync(ctx)

	// Wait for pipeline to stop (RunTopSync calls StopNow at end)
	// But StopNow is async or synchronous regarding some parts. 
	// Main should assume it blocks until done? RunTopSync blocks on WaitUntilIdle.
}
