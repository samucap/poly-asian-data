// Package pipeline orchestrates multi-stage data processing pipelines.
//
// Construction: use Factory.Create only.
// Stages: fetcher → processor → saver, with feedback (derived/pagination) via a sync router.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/processor"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrPipelineStopped = errors.New("pipeline has been stopped")
	ErrInvalidConfig   = errors.New("invalid pipeline configuration")
	ErrNotAccepting    = errors.New("pipeline is not accepting new work")
)

// =============================================================================
// Stats
// =============================================================================

// Stats contains pipeline statistics.
type Stats struct {
	StartedAt      time.Time
	UptimeDuration time.Duration
	Fetcher        workerpool.StatsSnapshot
	Processor      workerpool.StatsSnapshot
	Saver          saver.StatsSnapshot
}

// =============================================================================
// Pipeline
// =============================================================================

// Pipeline is one isolated instance of fetcher → processor → saver.
type Pipeline struct {
	name          string
	fetcherPool   *fetcher.Fetcher
	processorPool *processor.Processor
	saverPool     *saver.Saver
	logger        *slog.Logger
	startedAt     time.Time
	cfg           *config.Config

	ctx    context.Context
	cancel context.CancelFunc

	stopped        atomic.Bool
	accepting      atomic.Bool
	routerInflight atomic.Int64

	plyMktSvc *services.PlyMktService
}

// RunBatch seeds the fetcher with requests and waits until the pipeline is idle.
func (p *Pipeline) RunBatch(ctx context.Context, name string, reqs []*fetcher.Request) error {
	if !p.accepting.Load() || p.stopped.Load() {
		return ErrNotAccepting
	}
	p.logger.Info("running batch",
		slog.String("batch", name),
		slog.Int("count", len(reqs)),
	)
	for _, req := range reqs {
		if req == nil {
			continue
		}
		if err := p.fetcherPool.SubmitWait(ctx, req); err != nil {
			return err
		}
	}
	p.WaitUntilIdle(ctx, 500*time.Millisecond)
	return ctx.Err()
}

// SubmitFetch enqueues a single fetch request with backpressure.
func (p *Pipeline) SubmitFetch(ctx context.Context, req *fetcher.Request) error {
	if !p.accepting.Load() || p.stopped.Load() {
		return ErrNotAccepting
	}
	return p.fetcherPool.SubmitWait(ctx, req)
}

// SubmitSave enqueues a single save record with backpressure.
func (p *Pipeline) SubmitSave(ctx context.Context, record *saver.Record) error {
	if !p.accepting.Load() || p.stopped.Load() {
		return ErrNotAccepting
	}
	return p.saverPool.SubmitWait(ctx, record)
}

// TakeMergedOI returns conditionID → open-interest values collected by the
// processor when enrichment used Metadata MergeOI=true (top-markets path).
func (p *Pipeline) TakeMergedOI() map[string]float64 {
	return p.processorPool.TakeMergedOI()
}

func (p *Pipeline) logCycleComplete(phase string) {
	stats := p.Stats()
	p.logger.Info("cycle complete",
		slog.String("phase", phase),
		slog.String("fetched", logging.FormatCount(stats.Fetcher.Completed)),
		slog.String("processed", logging.FormatCount(stats.Processor.Completed)),
		slog.String("saved", logging.FormatCount(stats.Saver.RecordsSaved)),
	)
}
