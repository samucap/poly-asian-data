package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	internaldb "github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/processor"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// Options configures a single pipeline instance.
type Options struct {
	Name             string
	FetcherWorkers   int
	ProcessorWorkers int
	SaverWorkers     int
	FetcherQueue     int
	ProcessorQueue   int
	SaverQueue       int
	Logger           *slog.Logger
}

// Factory constructs isolated pipeline instances that share one DB pool.
type Factory struct {
	cfg    *config.Config
	logger *slog.Logger
	db     *pgxpool.Pool
}

// NewFactory opens a shared DB pool and initializes schema.
func NewFactory(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Factory, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is required", ErrInvalidConfig)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres config: %w", err)
	}
	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool: %w", err)
	}
	if err := internaldb.InitDB(ctx, db, false); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing db schema: %w", err)
	}

	return &Factory{
		cfg:    cfg,
		logger: logger.With(slog.String("component", "pipeline-factory")),
		db:     db,
	}, nil
}

// Create builds a fully wired pipeline instance.
func (f *Factory) Create(ctx context.Context, opts Options) (*Pipeline, error) {
	if f == nil || f.db == nil {
		return nil, fmt.Errorf("%w: factory is closed or nil", ErrInvalidConfig)
	}

	name := opts.Name
	if name == "" {
		name = "default"
	}
	logger := opts.Logger
	if logger == nil {
		logger = f.logger
	}
	logger = logger.With(slog.String("pipeline", name))

	fw, fq := pickWorkers(opts.FetcherWorkers, opts.FetcherQueue, f.cfg.FetcherCfg)
	pw, pq := pickWorkers(opts.ProcessorWorkers, opts.ProcessorQueue, f.cfg.ProcessorCfg)
	sw, sq := pickWorkers(opts.SaverWorkers, opts.SaverQueue, f.cfg.SaverCfg)

	pipelineCtx, cancel := context.WithCancel(ctx)

	fetcherPool, err := fetcher.New(pipelineCtx, f.cfg, fw, fq)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create fetcher: %w", err)
	}

	processorPool, err := processor.New(pipelineCtx, f.cfg, pw, pq)
	if err != nil {
		cancel()
		fetcherPool.StopNow()
		return nil, fmt.Errorf("create processor: %w", err)
	}

	saverLogger := logger.With(slog.String("component", "saver"))
	saverPool, err := saver.New(pipelineCtx, saverLogger, f.cfg, f.db, sw, sq)
	if err != nil {
		cancel()
		fetcherPool.StopNow()
		processorPool.StopNow()
		return nil, fmt.Errorf("create saver: %w", err)
	}

	p := &Pipeline{
		name:          name,
		fetcherPool:   fetcherPool,
		processorPool: processorPool,
		saverPool:     saverPool,
		logger:        logger.With(slog.String("component", "pipeline")),
		startedAt:     time.Now(),
		cfg:           f.cfg,
		ctx:           pipelineCtx,
		cancel:        cancel,
		plyMktSvc: &services.PlyMktService{
			Cfg:    f.cfg,
			Logger: logger,
			Ctx:    pipelineCtx,
		},
	}
	p.accepting.Store(true)

	// Wire stages with backpressure-preserving bridges.
	workerpool.StartBridge(pipelineCtx, fetcherPool.Outputs(), submitAdapter[*fetcher.Response]{
		submit: func(ctx context.Context, v *fetcher.Response) error {
			return processorPool.SubmitWait(ctx, v)
		},
	}, func(err error) {
		p.logger.Warn("fetcher→processor bridge", slog.String("error", err.Error()))
	})

	go p.routeProcessorOutput(pipelineCtx)
	workerpool.StartDrain(pipelineCtx, saverPool.Outputs())

	p.logger.Info("pipeline created",
		slog.Int("fetcher_workers", fw),
		slog.Int("processor_workers", pw),
		slog.Int("saver_workers", sw),
	)
	return p, nil
}

// Close releases the shared database pool. Call after all pipelines are stopped.
func (f *Factory) Close() {
	if f == nil || f.db == nil {
		return
	}
	f.db.Close()
	f.db = nil
}

// DB exposes the shared pool (e.g. for tests).
func (f *Factory) DB() *pgxpool.Pool {
	if f == nil {
		return nil
	}
	return f.db
}

func pickWorkers(workers, queue int, def config.WorkerPoolConfig) (int, int) {
	if workers <= 0 {
		workers = def.NumWorkers
	}
	if queue <= 0 {
		queue = def.Qsize
	}
	return workers, queue
}

// submitAdapter adapts a function to workerpool.Submitter.
type submitAdapter[T any] struct {
	submit func(ctx context.Context, data T) error
}

func (a submitAdapter[T]) SubmitWait(ctx context.Context, data T) error {
	return a.submit(ctx, data)
}
