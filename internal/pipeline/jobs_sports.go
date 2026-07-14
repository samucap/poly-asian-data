package pipeline

import (
	"log/slog"
)

// RunSportsTagsSync seeds sports/tags requests and waits for completion.
func (p *Pipeline) RunSportsTagsSync() {
	p.logger.Info("Starting PolyMarket Sync...")

	reqs, err := p.plyMktSvc.GetSportsReqs(p.ctx)
	if err != nil {
		p.logger.Error("failed to get sports reqs", slog.Any("error", err))
		return
	}

	if err := p.RunBatch(p.ctx, "sports_tags", reqs); err != nil {
		p.logger.Error("sports tags batch failed", slog.Any("error", err))
	}

	p.PrintFinalReport()
	p.Stop()
}
