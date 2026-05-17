package db

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// InitDB initializes the database schema.
// If forceReset is true, it drops all relevant tables before creating them.
func InitDB(ctx context.Context, pool *pgxpool.Pool, forceReset bool) error {
	if forceReset {
		dropQueries := []string{
			`DROP TABLE IF EXISTS prices_history CASCADE;`,
			`DROP TABLE IF EXISTS orderbooks CASCADE;`,
			`DROP TABLE IF EXISTS position_snapshots CASCADE;`,
			`DROP TABLE IF EXISTS enriched_order_filled_events CASCADE;`,
			`DROP TABLE IF EXISTS order_filled_events CASCADE;`,
			`DROP TABLE IF EXISTS accounts CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_markets CASCADE;`,
			`DROP TABLE IF EXISTS teams CASCADE;`,
			`DROP TABLE IF EXISTS leagues CASCADE;`,
			`DROP TABLE IF EXISTS tags CASCADE;`,
			`DROP TABLE IF EXISTS sports CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_holders CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_users CASCADE;`,
			`DROP TABLE IF EXISTS plymkt_leaderboard CASCADE;`,
		}
		for _, q := range dropQueries {
			if _, err := pool.Exec(ctx, q); err != nil {
				return fmt.Errorf("failed to drop table with query %q: %w", q, err)
			}
		}
	}

	// Split the schema SQL by semicolon to execute statements individually,
	// checking against empty strings to avoid errors.
	// Note: Simple split might be fragile if SQL contains semicolons in strings/functions.
	// However, for this specific schema.sql, a simple robust split or just executing the whole block
	// might work depending on driver support. pgx Exec usually handles multiple statements multiple calls
	// or one block? prefer one block if allowed, but sometimes mixed usage fails.
	// Let's try executing the whole block first.
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		// Fallback or better error handling?
		return fmt.Errorf("failed to execute schema sql: %w", err)
	}

	return nil
}
