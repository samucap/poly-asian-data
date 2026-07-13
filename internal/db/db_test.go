package db_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// cwd may be package dir (internal/db) or repo root depending on invocation
	_ = godotenv.Load(".env_test")
	_ = godotenv.Load("../../.env_test")
	os.Exit(m.Run())
}

func testPostgresURL(t *testing.T) string {
	t.Helper()
	connStr := os.Getenv("POSTGRES_URL")
	require.NotEmpty(t, connStr, "POSTGRES_URL must be set (load from .env_test)")
	return connStr
}

func TestStartDB_Success(t *testing.T) {
	t.Setenv("ENV", "prod") // ensure SSLMode from opts is preserved

	ctx := context.Background()
	opts := db.Options{
		ConnStr:           testPostgresURL(t),
		SSLMode:           "require",
		MaxConns:          10,
		MinConns:          2,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   15 * time.Minute,
		HealthCheckPeriod: time.Minute,
		ConnectTimeout:    5 * time.Second,
	}

	d, err := db.StartDB(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, d)

	assert.Equal(t, opts.SSLMode, d.SSLMode)
	assert.Equal(t, opts.MaxConns, d.MaxConns)
	assert.Equal(t, opts.MinConns, d.MinConns)
	assert.Equal(t, opts.MaxConnLifetime, d.MaxConnLifetime)
	assert.Equal(t, opts.MaxConnIdleTime, d.MaxConnIdleTime)
	assert.Equal(t, opts.HealthCheckPeriod, d.HealthCheckPeriod)
	assert.Equal(t, opts.ConnectTimeout, d.ConnectTimeout)
}

func TestStartDB_DevDisablesSSL(t *testing.T) {
	t.Setenv("ENV", "dev")

	d, err := db.StartDB(context.Background(), db.Options{
		ConnStr: testPostgresURL(t),
		SSLMode: "require",
	})
	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Equal(t, "disable", d.SSLMode)
}

func TestStartDB_EmptyConnStr(t *testing.T) {
	d, err := db.StartDB(context.Background(), db.Options{})
	assert.Nil(t, d)
	assert.ErrorIs(t, err, db.ErrEmptyConnStr)
}

func TestConnectDB_NilReceiver(t *testing.T) {
	var d *db.DB
	pool, err := d.ConnectDB(context.Background())
	assert.Nil(t, pool)
	assert.ErrorIs(t, err, db.ErrNilDB)
}

func TestConnectDB_InvalidConnStr(t *testing.T) {
	d, err := db.StartDB(context.Background(), db.Options{
		ConnStr: "postgres://%",
	})
	require.NoError(t, err)

	pool, err := d.ConnectDB(context.Background())
	assert.Nil(t, pool)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db connect")
}

func TestDBInterface_PoolImplements(t *testing.T) {
	// Compile-time coverage lives in db.go; this documents the contract in tests.
	var _ db.DBInterface = (*pgxpool.Pool)(nil)
}

func TestConnectDB_Integration(t *testing.T) {
	connStr := os.Getenv("POSTGRES_URL")
	if connStr == "" {
		t.Skip("POSTGRES_URL not set after loading .env_test; skipping integration test")
	}

	ctx := context.Background()
	d, err := db.StartDB(ctx, db.Options{
		ConnStr:        connStr,
		ConnectTimeout: 5 * time.Second,
		MaxConns:       2,
		MinConns:       1,
	})
	require.NoError(t, err)

	pool, err := d.ConnectDB(ctx)
	if err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}
	defer pool.Close()

	require.NoError(t, pool.Ping(ctx))
	assert.NotNil(t, pool)
}

func TestStartDB_ErrorsAreDistinct(t *testing.T) {
	assert.False(t, errors.Is(db.ErrEmptyConnStr, db.ErrNilDB))
}
