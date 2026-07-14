package pipeline

import (
	"context"
	"log/slog"
	"time"
)

// WaitUntilIdle blocks until the pipeline has no pending stage work and the
// router is not mid-item, continuously for stableDuration.
func (p *Pipeline) WaitUntilIdle(ctx context.Context, stableDuration time.Duration) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var stableSince time.Time
	isStable := false
	lastLog := time.Time{}
	logInterval := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idle := p.isIdle()

			if idle && !isStable {
				p.logger.Info("pipeline became IDLE; starting stability timer")
				stableSince = time.Now()
				isStable = true
				lastLog = time.Now()
			} else if !idle && isStable {
				p.logger.Info("pipeline stability RESET; activity detected")
				isStable = false
			} else if !idle && time.Since(lastLog) >= logInterval {
				p.logStatus(idle, isStable, stableSince)
				lastLog = time.Now()
			}

			if idle && isStable && time.Since(stableSince) >= stableDuration {
				return
			}
		}
	}
}

func (p *Pipeline) isIdle() bool {
	if p.routerInflight.Load() != 0 {
		return false
	}
	return p.fetcherPool.Pending() == 0 &&
		p.processorPool.Pending() == 0 &&
		p.saverPool.Pending() == 0
}

func (p *Pipeline) logStatus(idle, stable bool, stableSince time.Time) {
	fs := p.fetcherPool.Stats().Snapshot()
	ps := p.processorPool.Stats().Snapshot()
	ss := p.saverPool.Stats().Snapshot()
	p.logger.Info("Pipeline Status",
		slog.Int64("fetcher_pending", fs.Pending()),
		slog.Int64("processor_pending", ps.Pending()),
		slog.Int64("saver_pending", ss.Pending()),
		slog.Int64("router_inflight", p.routerInflight.Load()),
		slog.Bool("idle", idle),
		slog.Bool("stable", stable),
		slog.Duration("stable_for", time.Since(stableSince)),
	)
}

// Stats returns current pipeline statistics.
func (p *Pipeline) Stats() Stats {
	return Stats{
		StartedAt:      p.startedAt,
		UptimeDuration: time.Since(p.startedAt),
		Fetcher:        p.fetcherPool.Stats().Snapshot(),
		Processor:      p.processorPool.Stats().Snapshot(),
		Saver:          p.saverPool.SaverStats().Snapshot(),
	}
}

// PrintFinalReport logs a summary of pipeline execution.
func (p *Pipeline) PrintFinalReport() {
	stats := p.Stats()
	p.logger.Info("==================================================")
	p.logger.Info("           PIPELINE END REPORT                  ")
	p.logger.Info("==================================================")
	p.logger.Info("Execution Time", slog.Duration("duration", stats.UptimeDuration))
	p.logger.Info("Fetcher",
		slog.Int64("submitted", stats.Fetcher.Submitted),
		slog.Int64("completed", stats.Fetcher.Completed),
		slog.Int64("failed", stats.Fetcher.Failed),
	)
	p.logger.Info("Processor",
		slog.Int64("submitted", stats.Processor.Submitted),
		slog.Int64("completed", stats.Processor.Completed),
		slog.Int64("failed", stats.Processor.Failed),
	)
	p.logger.Info("Saver",
		slog.Int64("submitted", stats.Saver.RecordsSubmitted),
		slog.Int64("saved", stats.Saver.RecordsSaved),
		slog.Int64("failed", stats.Saver.RecordsFailed),
		slog.Int64("rows", stats.Saver.RowsAffected),
	)
	p.logger.Info("==================================================")
}

// IsStopped returns true if the pipeline has been stopped.
func (p *Pipeline) IsStopped() bool {
	return p.stopped.Load()
}

// Stop gracefully shuts down: stop accepting seeds, wait until idle, stop stages.
func (p *Pipeline) Stop() {
	if p.stopped.Swap(true) {
		return
	}
	p.accepting.Store(false)
	p.logger.Info("pipeline stopping (graceful)...")

	// Best-effort drain of in-flight work using pipeline ctx (may already be live).
	drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p.WaitUntilIdle(drainCtx, 500*time.Millisecond)

	p.fetcherPool.Stop()
	p.processorPool.Stop()
	p.saverPool.Stop()
	p.cancel()

	stats := p.Stats()
	p.logger.Info("pipeline stopped",
		slog.Int64("fetched", stats.Fetcher.Completed),
		slog.Int64("processed", stats.Processor.Completed),
		slog.Int64("saved", stats.Saver.RecordsSaved),
		slog.Duration("uptime", stats.UptimeDuration),
	)
}

// StopNow immediately cancels and stops all stages.
func (p *Pipeline) StopNow() {
	if p.stopped.Swap(true) {
		return
	}
	p.accepting.Store(false)
	p.cancel()
	p.fetcherPool.StopNow()
	p.processorPool.StopNow()
	p.saverPool.StopNow()
}
