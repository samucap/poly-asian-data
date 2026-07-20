package pipeline

import (
	"context"
	"log/slog"
)

// RunSubgraphSync starts subgraph data sync (incremental cursors).
func (p *Pipeline) RunSubgraphSync(ctx context.Context) {
	p.RunSubgraphSyncWithOpts(ctx, false)
}

// RunSubgraphSyncWithOpts starts subgraph sync; fullSync ignores saved cursors.
func (p *Pipeline) RunSubgraphSyncWithOpts(ctx context.Context, fullSync bool) {
	p.logger.Info("Starting Subgraph Sync...", slog.Bool("fullSync", fullSync))

	targets := []string{"fpmms"}

	var startIds map[string]string
	if !fullSync {
		cursors, err := p.saverPool.GetAllSyncCursors(ctx)
		if err != nil {
			p.logger.Warn("failed to load sync cursors, starting fresh", slog.Any("error", err))
		} else if len(cursors) > 0 {
			startIds = cursors
			p.logger.Info("Loaded sync cursors for incremental sync",
				slog.Int("count", len(cursors)),
				slog.Any("cursors", cursors),
			)
		}
	} else {
		for _, target := range targets {
			if err := p.saverPool.ResetSyncCursor(ctx, target); err != nil {
				p.logger.Warn("failed to reset cursor", slog.String("target", target), slog.Any("error", err))
			}
		}
	}

	reqs, err := p.plyMktSvc.GetSubgraphReqs(ctx, targets, startIds)
	if err != nil {
		p.logger.Error("failed to get subgraph reqs", slog.Any("error", err))
		return
	}

	if err := p.RunBatch(ctx, "subgraph", reqs); err != nil {
		p.logger.Error("subgraph batch failed", slog.Any("error", err))
	}

	for _, target := range targets {
		if err := p.saverPool.MarkSyncComplete(ctx, target); err != nil {
			p.logger.Warn("failed to mark sync complete", slog.String("target", target), slog.Any("error", err))
		}
	}

	p.PrintFinalReport()
	p.Stop()
}
