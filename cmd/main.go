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

	env := os.Getenv("ENV")
	if env == "" {
		env = "dev"
	}
	logging.Init(env)

	cfg, err := config.Load()
	if err != nil {
		logging.Error("Failed to load configuration", slog.Any("error", err))
		os.Exit(1)
	}
	logging.Init(cfg.ENV)

	logging.Info("Starting Poly Asian Data Pipeline...",
		slog.String("environment", cfg.ENV),
	)

	factory, err := pipeline.NewFactory(ctx, cfg, logging.Logger)
	if err != nil {
		logging.Error("Failed to create pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()

	pipe, err := factory.Create(ctx, pipeline.Options{Name: "sports-tags"})
	if err != nil {
		logging.Error("Failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logging.Info("Shutdown signal received")
		cancel()
		pipe.Stop()
	}()

	pipe.RunSportsTagsSync()
}
