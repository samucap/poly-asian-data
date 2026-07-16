package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/workerpool"
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
				p.logger.Debug("pipeline became IDLE; starting stability timer")
				stableSince = time.Now()
				isStable = true
				lastLog = time.Now()
			} else if !idle && isStable {
				p.logger.Debug("pipeline stability RESET; activity detected")
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
	p.logger.Debug("Pipeline Status",
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

// DiffStats returns after − before for cumulative counters (for cycle deltas).
func DiffStats(before, after Stats) Stats {
	return Stats{
		StartedAt:      after.StartedAt,
		UptimeDuration: after.UptimeDuration,
		Fetcher: workerpool.StatsSnapshot{
			Submitted:     after.Fetcher.Submitted - before.Fetcher.Submitted,
			Completed:     after.Fetcher.Completed - before.Fetcher.Completed,
			Failed:        after.Fetcher.Failed - before.Fetcher.Failed,
			InProgress:    after.Fetcher.InProgress,
			TotalDuration: after.Fetcher.TotalDuration - before.Fetcher.TotalDuration,
		},
		Processor: workerpool.StatsSnapshot{
			Submitted:     after.Processor.Submitted - before.Processor.Submitted,
			Completed:     after.Processor.Completed - before.Processor.Completed,
			Failed:        after.Processor.Failed - before.Processor.Failed,
			InProgress:    after.Processor.InProgress,
			TotalDuration: after.Processor.TotalDuration - before.Processor.TotalDuration,
		},
		Saver: saver.StatsSnapshot{
			RecordsSubmitted: after.Saver.RecordsSubmitted - before.Saver.RecordsSubmitted,
			RecordsSaved:     after.Saver.RecordsSaved - before.Saver.RecordsSaved,
			RecordsFailed:    after.Saver.RecordsFailed - before.Saver.RecordsFailed,
			RowsAffected:     after.Saver.RowsAffected - before.Saver.RowsAffected,
			TotalDuration:    after.Saver.TotalDuration - before.Saver.TotalDuration,
		},
	}
}

// LogStageReport logs compact aggregated counters for each pipeline stage:
// how many ops were submitted, how many succeeded/failed, success rate, avg latency.
// Safe for cycle-end use only (formatting is intentional here, not in workers).
func (p *Pipeline) LogStageReport(label string, s Stats) {
	log := p.logger
	if log == nil {
		log = logging.Logger
	}
	if label == "" {
		label = "pipeline"
	}

	fetcherAvg := s.Fetcher.AverageDuration()
	processorAvg := s.Processor.AverageDuration()
	var saverAvg time.Duration
	finished := s.Saver.RecordsSaved + s.Saver.RecordsFailed
	if finished > 0 {
		saverAvg = s.Saver.TotalDuration / time.Duration(finished)
	}

	log.Info("stage fetcher",
		slog.String("report", label),
		slog.String("ops", logging.FormatCount(s.Fetcher.Submitted)),
		slog.String("ok", logging.FormatCount(s.Fetcher.Completed)),
		slog.String("failed", logging.FormatCount(s.Fetcher.Failed)),
		slog.String("success", logging.FormatRate(s.Fetcher.Completed, s.Fetcher.Failed)),
		slog.String("avg", logging.FormatDuration(fetcherAvg)),
	)
	log.Info("stage processor",
		slog.String("report", label),
		slog.String("ops", logging.FormatCount(s.Processor.Submitted)),
		slog.String("ok", logging.FormatCount(s.Processor.Completed)),
		slog.String("failed", logging.FormatCount(s.Processor.Failed)),
		slog.String("success", logging.FormatRate(s.Processor.Completed, s.Processor.Failed)),
		slog.String("avg", logging.FormatDuration(processorAvg)),
	)
	log.Info("stage saver",
		slog.String("report", label),
		slog.String("ops", logging.FormatCount(s.Saver.RecordsSubmitted)),
		slog.String("ok", logging.FormatCount(s.Saver.RecordsSaved)),
		slog.String("failed", logging.FormatCount(s.Saver.RecordsFailed)),
		slog.String("success", logging.FormatRate(s.Saver.RecordsSaved, s.Saver.RecordsFailed)),
		slog.String("rows", logging.FormatCount(s.Saver.RowsAffected)),
		slog.String("avg", logging.FormatDuration(saverAvg)),
	)
}

// PrintFinalReport logs a cumulative end-of-run stage report (absolute totals).
func (p *Pipeline) PrintFinalReport() {
	stats := p.Stats()
	p.logger.Info("pipeline end report",
		slog.String("duration", logging.FormatDuration(stats.UptimeDuration)),
	)
	p.LogStageReport("total", stats)
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
		slog.String("fetched", logging.FormatCount(stats.Fetcher.Completed)),
		slog.String("processed", logging.FormatCount(stats.Processor.Completed)),
		slog.String("saved", logging.FormatCount(stats.Saver.RecordsSaved)),
		slog.String("uptime", logging.FormatDuration(stats.UptimeDuration)),
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
