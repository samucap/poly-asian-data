package pipeline

import (
	"fmt"
	"log/slog"
)

// SyncSportsTags seeds sports/tags requests and waits for completion.
// Does not stop the pipeline (safe to call from catalog-markets or other long-lived cmds).
func (p *Pipeline) SyncSportsTags() error {
	p.logger.Info("Starting sports/tags sync...")

	reqs, err := p.plyMktSvc.GetSportsReqs(p.ctx)
	if err != nil {
		return fmt.Errorf("sports/tags: get requests: %w", err)
	}

	if err := p.RunBatch(p.ctx, "sports_tags", reqs); err != nil {
		return fmt.Errorf("sports/tags batch: %w", err)
	}
	return nil
}

// RunSportsTagsSync runs sports/tags sync then stops the pipeline (cmd/sports-sync entrypoint).
func (p *Pipeline) RunSportsTagsSync() {
	if err := p.SyncSportsTags(); err != nil {
		p.logger.Error("sports/tags sync failed", slog.Any("error", err))
	}
	p.PrintFinalReport()
	p.Stop()
}
