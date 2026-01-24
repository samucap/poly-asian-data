// Package saver provides a database client wrapper for saving processed data.
// Uses the generic workerpool.Pool for worker management.
package saver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPoolStopped   = errors.New("saver pool has been stopped")
	ErrInvalidConfig = errors.New("invalid saver configuration")
	ErrSaveFailed    = errors.New("save failed")
)

// =============================================================================
// Type Definitions
// =============================================================================

type Saver struct {
	*workerpool.Pool[*Record, *Result]
	db     *pgxpool.Pool
	logger *slog.Logger
	stats  Stats
}

// Record represents data to be saved.
type Record struct {
	ID          string
	TableName   string
	Data        any
	ItemCount   int
	ProcessedAt time.Time
}

// Result represents the outcome of a save operation.
type Result struct {
	ID           string
	RowsAffected int64
	Duration     time.Duration
}

// Stats contains atomic counters for saver statistics.
type Stats struct {
	RecordsSubmitted atomic.Int64
	RecordsSaved     atomic.Int64
	RecordsFailed    atomic.Int64
	RowsAffected     atomic.Int64
	TotalDuration    atomic.Int64
}

// Snapshot returns a point-in-time copy.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		RecordsSubmitted: s.RecordsSubmitted.Load(),
		RecordsSaved:     s.RecordsSaved.Load(),
		RecordsFailed:    s.RecordsFailed.Load(),
		RowsAffected:     s.RowsAffected.Load(),
		TotalDuration:    time.Duration(s.TotalDuration.Load()),
	}
}

// StatsSnapshot is an immutable snapshot.
type StatsSnapshot struct {
	RecordsSubmitted int64
	RecordsSaved     int64
	RecordsFailed    int64
	RowsAffected     int64
	TotalDuration    time.Duration
}

// =============================================================================
// Constructor
// =============================================================================

// New creates and initializes a saver pool with a PostgreSQL connection.
func New(ctx context.Context, cfg *config.Config, numWorkers, qSize int) (*Saver, error) {
	logger := logging.Logger.With(
		slog.String("component", "saver"),
	)

	// Initialize pgx connection pool
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres config: %w", err)
	}

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool: %w", err)
	}

	s := &Saver{
		db:     db,
		logger: logger,
	}

	pool, err := workerpool.NewPool[*Record, *Result](ctx, "saver", numWorkers, qSize, logger, s.workerTask)
	if err != nil {
		db.Close()
		return nil, err
	}

	s.Pool = pool

	logger.Info("saver initialized",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return s, nil
}

// Close closes the database connection pool.
func (s *Saver) Close() {
	s.db.Close()
}

// SaverStats returns statistics.
func (s *Saver) SaverStats() *Stats {
	return &s.stats
}

// =============================================================================
// Worker Task
// =============================================================================

func (s *Saver) workerTask(ctx context.Context, record *Record) (*Result, error) {
	start := time.Now()

	s.logger.Info("saving record",
		slog.String("table", record.TableName),
		slog.Int("itemCount", record.ItemCount),
	)

	var rowsAffected int64
	var err error

	switch record.TableName {
	case "sport":
		rowsAffected, err = s.batchInsertSports(ctx, record.Data)
	case "team":
		rowsAffected, err = s.batchInsertTeams(ctx, record.Data)
	default:
		return nil, fmt.Errorf("unknown table: %s", record.TableName)
	}

	if err != nil {
		s.stats.RecordsFailed.Add(1)
		s.logger.Error("save failed",
			slog.String("table", record.TableName),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	duration := time.Since(start)
	s.stats.RecordsSaved.Add(int64(record.ItemCount))
	s.stats.RowsAffected.Add(rowsAffected)
	s.stats.TotalDuration.Add(int64(duration))

	s.logger.Info("saved record",
		slog.String("table", record.TableName),
		slog.Int64("rowsAffected", rowsAffected),
		slog.Duration("duration", duration),
	)

	return &Result{
		ID:           record.ID,
		RowsAffected: rowsAffected,
		Duration:     duration,
	}, nil
}

// =============================================================================
// Batch Insert Methods
// =============================================================================

func (s *Saver) batchInsertSports(ctx context.Context, data any) (int64, error) {
	sports, ok := data.([]services.PlyMktSport)
	if !ok {
		return 0, fmt.Errorf("invalid data type for sports: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, sport := range sports {
		batch.Queue(`
			INSERT INTO sport (sport, image, resolution, ordering, tags, series)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (sport) DO UPDATE SET
				image = EXCLUDED.image,
				resolution = EXCLUDED.resolution,
				ordering = EXCLUDED.ordering,
				tags = EXCLUDED.tags,
				series = EXCLUDED.series
		`, sport.Sport, sport.Image, sport.Resolution, sport.Ordering, sport.Tags, sport.Series)
	}

	return s.execBatch(ctx, batch, len(sports))
}

func (s *Saver) batchInsertTeams(ctx context.Context, data any) (int64, error) {
	teams, ok := data.([]services.PlyMktTeam)
	if !ok {
		return 0, fmt.Errorf("invalid data type for teams: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, t := range teams {
		batch.Queue(`
			INSERT INTO team (id, name, league, record, logo, abbreviation, alias, provider_id, color)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (id) DO UPDATE SET
				name = EXCLUDED.name,
				league = EXCLUDED.league,
				record = EXCLUDED.record,
				logo = EXCLUDED.logo,
				abbreviation = EXCLUDED.abbreviation,
				alias = EXCLUDED.alias,
				provider_id = EXCLUDED.provider_id,
				color = EXCLUDED.color
		`, t.ID, t.Name, t.League, t.Record, t.Logo, t.Abbreviation, t.Alias, t.ProviderID, t.Color)
	}

	return s.execBatch(ctx, batch, len(teams))
}

// execBatch executes a batch and returns total rows affected.
func (s *Saver) execBatch(ctx context.Context, batch *pgx.Batch, count int) (int64, error) {
	results := s.db.SendBatch(ctx, batch)
	defer results.Close()

	var totalAffected int64
	for i := 0; i < count; i++ {
		ct, err := results.Exec()
		if err != nil {
			return totalAffected, fmt.Errorf("batch exec at index %d: %w", i, err)
		}
		totalAffected += ct.RowsAffected()
	}

	return totalAffected, nil
}

// resolveTableName determines the table name from data type.
func resolveTableName(data any) string {
	v := reflect.TypeOf(data)
	if v.Kind() == reflect.Slice {
		v = v.Elem()
	}
	switch v.Name() {
	case "PlyMktSport":
		return "sport"
	case "PlyMktTeam":
		return "team"
	default:
		return ""
	}
}