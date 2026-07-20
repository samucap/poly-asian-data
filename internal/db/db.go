// Package db provides a reusable PostgreSQL connection pool setup.
// Callers supply connection parameters. The only env var read is ENV:
// when ENV=dev, SSL is forced off (sslmode=disable).
package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Ensure *pgxpool.Pool continues to satisfy DBInterface.
var _ DBInterface = (*pgxpool.Pool)(nil)

var (
	// ErrEmptyConnStr is returned when Options.ConnStr is empty.
	ErrEmptyConnStr = errors.New("db: empty connection string")
	// ErrNilDB is returned when ConnectDB is called on a nil receiver.
	ErrNilDB = errors.New("db: nil DB")
)

// Options holds caller-supplied settings used by StartDB to construct a DB.
type Options struct {
	ConnStr           string // required
	SSLMode           string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

// DB holds connection settings used to open a pool via ConnectDB.
// It is not itself a live connection; ConnectDB returns *pgxpool.Pool.
type DB struct {
	connStr           string
	SSLMode           string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

// DBInterface is the injectable CRUD surface for a live pool/connection.
// *pgxpool.Pool implements this interface.
type DBInterface interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Begin(ctx context.Context) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// StartDB validates opts and returns a configured *DB.
// ctx is reserved for future use (construction is currently pure).
// When ENV=dev, SSL is disabled regardless of opts.SSLMode.
func StartDB(ctx context.Context, opts Options) (*DB, error) {
	_ = ctx

	if opts.ConnStr == "" {
		return nil, ErrEmptyConnStr
	}

	sslMode := opts.SSLMode
	if isDevEnv() {
		sslMode = "disable"
	}

	return &DB{
		connStr:           opts.ConnStr,
		SSLMode:           sslMode,
		MaxConns:          opts.MaxConns,
		MinConns:          opts.MinConns,
		MaxConnLifetime:   opts.MaxConnLifetime,
		MaxConnIdleTime:   opts.MaxConnIdleTime,
		HealthCheckPeriod: opts.HealthCheckPeriod,
		ConnectTimeout:    opts.ConnectTimeout,
	}, nil
}

// ConnectDB opens a connection pool using the settings on db, pings it, and returns the pool.
func (db *DB) ConnectDB(ctx context.Context) (*pgxpool.Pool, error) {
	if db == nil {
		return nil, ErrNilDB
	}

	// In dev, always force sslmode=disable (including if the DSN already had SSL enabled).
	var dsn string
	if isDevEnv() {
		dsn = setSSLMode(db.connStr, "disable")
	} else {
		dsn = applySSLMode(db.connStr, db.SSLMode)
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("db connect: parse config: %w", err)
	}

	if db.MaxConns > 0 {
		poolCfg.MaxConns = db.MaxConns
	}
	if db.MinConns > 0 {
		poolCfg.MinConns = db.MinConns
	}
	if db.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = db.MaxConnLifetime
	}
	if db.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = db.MaxConnIdleTime
	}
	if db.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = db.HealthCheckPeriod
	}

	connectCtx := ctx
	if db.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		connectCtx, cancel = context.WithTimeout(ctx, db.ConnectTimeout)
		defer cancel()
	}

	pool, err := pgxpool.NewWithConfig(connectCtx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}

	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db connect: ping: %w", err)
	}

	return pool, nil
}

func isDevEnv() bool {
	return os.Getenv("ENV") == "dev"
}

// applySSLMode appends sslmode to the DSN when SSLMode is set and the DSN has no sslmode yet.
func applySSLMode(connStr, sslMode string) string {
	if sslMode == "" {
		return connStr
	}
	if strings.Contains(strings.ToLower(connStr), "sslmode=") {
		return connStr
	}
	return setSSLMode(connStr, sslMode)
}

// setSSLMode sets or replaces sslmode on a URL-style or keyword/value DSN.
func setSSLMode(connStr, sslMode string) string {
	if sslMode == "" {
		return connStr
	}

	// URL-style DSN: postgres://...
	if u, err := url.Parse(connStr); err == nil && u.Scheme != "" && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		q := u.Query()
		q.Set("sslmode", sslMode)
		u.RawQuery = q.Encode()
		return u.String()
	}

	// Keyword/value DSN: host=... user=...
	lower := strings.ToLower(connStr)
	if strings.Contains(lower, "sslmode=") {
		parts := strings.Fields(connStr)
		for i, p := range parts {
			if strings.HasPrefix(strings.ToLower(p), "sslmode=") {
				parts[i] = "sslmode=" + sslMode
			}
		}
		return strings.Join(parts, " ")
	}
	return strings.TrimSpace(connStr) + " sslmode=" + sslMode
}
