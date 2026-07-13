package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchExec_EmptyRows(t *testing.T) {
	// empty rows is a no-op even with a nil conn
	assert.NoError(t, db.BatchExec(context.Background(), nil, "SELECT 1", nil))
	assert.NoError(t, db.BatchExec(context.Background(), nil, "SELECT 1", [][]any{}))
}

func TestBatchExec_NilConn(t *testing.T) {
	err := db.BatchExec(context.Background(), nil, "SELECT 1", [][]any{{1}})
	assert.ErrorIs(t, err, db.ErrNilDB)
}

func TestBatchExec_InsertAndUpsert(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)

	require.NoError(t, db.InitDB(ctx, pool, false))

	id1 := fmt.Sprintf("test-batch-%d-a", time.Now().UnixNano())
	id2 := fmt.Sprintf("test-batch-%d-b", time.Now().UnixNano())
	ids := []string{id1, id2}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tags WHERE id = ANY($1)`, ids)
	})

	sql := `INSERT INTO tags (id, label, slug)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label`

	require.NoError(t, db.BatchExec(ctx, pool, sql, [][]any{
		{id1, "A", "a"},
		{id2, "B", "b"},
	}))

	require.NoError(t, db.BatchExec(ctx, pool, sql, [][]any{
		{id1, "A2", "a"},
	}))

	var label string
	err := pool.QueryRow(ctx, `SELECT label FROM tags WHERE id = $1`, id1).Scan(&label)
	require.NoError(t, err)
	assert.Equal(t, "A2", label)

	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM tags WHERE id = ANY($1)`, ids).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestBatchExec_MultipleStatementsSameSQL(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)
	require.NoError(t, db.InitDB(ctx, pool, false))

	prefix := fmt.Sprintf("test-multi-%d", time.Now().UnixNano())
	sql := `INSERT INTO tags (id, label, slug) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING`

	const n = 5
	rows := make([][]any, 0, n)
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		ids = append(ids, id)
		rows = append(rows, []any{id, fmt.Sprintf("L%d", i), fmt.Sprintf("s%d", i)})
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tags WHERE id = ANY($1)`, ids)
	})

	require.NoError(t, db.BatchExec(ctx, pool, sql, rows))

	var count int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM tags WHERE id = ANY($1)`, ids).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, n, count)
}

func TestBatchExec_ItemError(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)

	// Wrong number of args for placeholders
	err := db.BatchExec(ctx, pool, `INSERT INTO tags (id, label) VALUES ($1, $2)`, [][]any{
		{"only-one-arg"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db batch item")

	var be *db.BatchItemError
	require.ErrorAs(t, err, &be)
	assert.Equal(t, 0, be.Index)
	assert.NotNil(t, be.Err)
}

func TestInitDB_CreatesGroundTables(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)
	require.NoError(t, db.InitDB(ctx, pool, false))

	tables := []string{
		"tags",
		"trades",
		"hot_events",
		"prices_history",
		"orderbook_snapshots",
		"plymkt_markets",
		"plymkt_events",
	}
	for _, name := range tables {
		var reg *string
		err := pool.QueryRow(ctx, `SELECT to_regclass($1)::text`, "public."+name).Scan(&reg)
		require.NoError(t, err, "table %s", name)
		require.NotNil(t, reg, "table %s missing", name)
		assert.Equal(t, name, *reg)
	}
}
