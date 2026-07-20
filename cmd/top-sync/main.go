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
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	logging.Init(cfg.ENV)
	logger := logging.Logger

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to create pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()

	pipe, err := factory.Create(ctx, pipeline.Options{Name: "top-sync"})
	if err != nil {
		logger.Error("failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("received signal, stopping pipeline...")
		cancel()
		pipe.Stop()
	}()

	pipe.RunTopSync(ctx)
}
