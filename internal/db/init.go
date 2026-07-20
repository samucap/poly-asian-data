package db

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// InitDB initializes the database schema.
// If forceReset is true, it drops all relevant tables before creating them.
// Statements run one at a time with the simple protocol so Timescale continuous
// aggregates / materialized views are not wrapped in an implicit transaction.
func InitDB(ctx context.Context, pool *pgxpool.Pool, forceReset bool) error {
	if forceReset {
		dropQueries := []string{
			`DROP MATERIALIZED VIEW IF EXISTS oi_hourly CASCADE;`,
			`DROP MATERIALIZED VIEW IF EXISTS market_pressure_1h CASCADE;`,
			`DROP MATERIALIZED VIEW IF EXISTS whale_rankings CASCADE;`,
			`DROP TABLE IF EXISTS oi_history CASCADE;`,
			`DROP TABLE IF EXISTS hot_events CASCADE;`,
			`DROP TABLE IF EXISTS trades CASCADE;`,
			`DROP TABLE IF EXISTS prices_history CASCADE;`,
			`DROP TABLE IF EXISTS orderbook_snapshots CASCADE;`,
			`DROP TABLE IF EXISTS hot_markets_vol CASCADE;`,
			`DROP TABLE IF EXISTS orderbooks CASCADE;`,
			`DROP TABLE IF EXISTS position_snapshots CASCADE;`,
			`DROP TABLE IF EXISTS enriched_order_filled_events CASCADE;`,
			`DROP TABLE IF EXISTS order_filled_events CASCADE;`,
			`DROP TABLE IF EXISTS accounts CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_markets CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_events CASCADE;`,
			`DROP TABLE IF EXISTS conditions CASCADE;`,
			`DROP TABLE IF EXISTS teams CASCADE;`,
			`DROP TABLE IF EXISTS leagues CASCADE;`,
			`DROP TABLE IF EXISTS tags CASCADE;`,
			`DROP TABLE IF EXISTS sports CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_holders CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_users CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_leaderboard CASCADE;`,
			`DROP TABLE IF EXISTS sync_state CASCADE;`,
		}
		for _, q := range dropQueries {
			if err := execSimple(ctx, pool, q); err != nil {
				return fmt.Errorf("failed to drop with query %q: %w", q, err)
			}
		}
	}

	for i, stmt := range splitSQLStatements(schemaSQL) {
		if err := execSimple(ctx, pool, stmt); err != nil {
			if isIgnorableSchemaError(err) {
				continue
			}
			preview := stmt
			if len(preview) > 120 {
				preview = preview[:120] + "..."
			}
			return fmt.Errorf("failed to execute schema statement %d (%s): %w", i+1, preview, err)
		}
	}

	return nil
}

func execSimple(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	_, err := pool.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	return err
}

// isIgnorableSchemaError treats re-entrant schema applies as success.
func isIgnorableSchemaError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "42P07", "42710": // duplicate_table / duplicate_object
		return true
	case "42P16": // invalid_table_definition sometimes on re-hypertable
		return strings.Contains(strings.ToLower(pgErr.Message), "already")
	}

	// Timescale: continuous aggregate / policy already exists style messages
	msg := strings.ToLower(pgErr.Message)
	return strings.Contains(msg, "already exists")
}

// splitSQLStatements splits schema SQL on semicolons outside of dollar-quoted
// or single-quoted strings, ignoring -- line comments (quotes in comments must not
// affect parsing).
func splitSQLStatements(sql string) []string {
	var stmts []string
	var b strings.Builder
	inSingle := false
	inDollar := false
	inLineComment := false

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		if inLineComment {
			b.WriteByte(c)
			if c == '\n' {
				inLineComment = false
			}
			continue
		}

		// Line comment -- (only when not in string/dollar quote)
		if !inSingle && !inDollar && c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			b.WriteByte(c)
			b.WriteByte(sql[i+1])
			i++
			inLineComment = true
			continue
		}

		if !inSingle && i+1 < len(sql) && sql[i] == '$' && sql[i+1] == '$' {
			b.WriteString("$$")
			i++
			inDollar = !inDollar
			continue
		}

		if !inDollar && c == '\'' {
			b.WriteByte(c)
			if inSingle && i+1 < len(sql) && sql[i+1] == '\'' {
				b.WriteByte(sql[i+1])
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}

		if c == ';' && !inSingle && !inDollar {
			stmt := strings.TrimSpace(b.String())
			b.Reset()
			if stmt != "" && !isSQLCommentOnly(stmt) {
				stmts = append(stmts, stmt)
			}
			continue
		}

		b.WriteByte(c)
	}

	if tail := strings.TrimSpace(b.String()); tail != "" && !isSQLCommentOnly(tail) {
		stmts = append(stmts, tail)
	}
	return stmts
}

func isSQLCommentOnly(s string) bool {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		return false
	}
	return true
}
