package pipeline

import (
	"context"
	"log/slog"

	"github.com/samucap/poly-asian-data/internal/services"
)

// RunTopSync seeds leaderboard + holders requests and waits for completion.
func (p *Pipeline) RunTopSync(ctx context.Context) {
	p.logger.Info("Starting Top Sync Pipeline (Leaderboard & Holders)...")

	windows := []string{"all", "month", "week"}
	for _, w := range windows {
		params := services.PlyMktLeaderboardParams{
			TimePeriod: w,
			Limit:      100,
		}
		reqs, err := p.plyMktSvc.GetLeaderboardReqs(ctx, params)
		if err != nil {
			p.logger.Error("failed to get leaderboard reqs", slog.String("window", w), slog.Any("error", err))
			continue
		}
		if err := p.RunBatch(ctx, "leaderboard_"+w, reqs); err != nil {
			p.logger.Error("leaderboard batch failed", slog.String("window", w), slog.Any("error", err))
		}
	}

	marketIDs, err := p.saverPool.GetActiveMarketIDs(ctx, 100)
	if err != nil {
		p.logger.Error("failed to get active market IDs for holders", slog.Any("error", err))
	} else if len(marketIDs) > 0 {
		reqs, err := p.plyMktSvc.GetHoldersReqs(ctx, marketIDs)
		if err != nil {
			p.logger.Error("failed to get holders reqs", slog.Any("error", err))
		} else if err := p.RunBatch(ctx, "holders", reqs); err != nil {
			p.logger.Error("holders batch failed", slog.Any("error", err))
		}
	} else {
		p.logger.Info("No active markets found, skipping holders sync")
	}

	p.PrintFinalReport()
	p.Stop()
}
