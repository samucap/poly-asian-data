package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
)

// TestMonitorQueries runs the queries used in monitor-whales against the DB.
// This is an integration test and requires a valid DB connection.
func TestMonitorQueries(t *testing.T) {
	// Setup
	os.Setenv("ENV", "dev")
	// Hack: Move to root to find .env
	os.Chdir("../..")
	
	logging.Init("dev")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	db, err := pgxpool.New(context.Background(), cfg.PostgresURL)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	defer db.Close()

	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("failed to ping db, skipping test: %v", err)
	}

	ctx := context.Background()

	t.Run("SyncStatus", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT sync_type, last_cursor, last_sync_at, total_items, status FROM sync_state")
		if err != nil {
			t.Fatalf("Sync Status Query Failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
			var typ, cursor, status string
			var lastSync time.Time
			var totalItems int
			if err := rows.Scan(&typ, &cursor, &lastSync, &totalItems, &status); err != nil {
				t.Errorf("Scan failed: %v", err)
			}
			t.Logf("SyncType: %s, Items: %d, Status: %s", typ, totalItems, status)
		}
		t.Logf("Found %d sync state records", count)
	})

	t.Run("WhaleFills", func(t *testing.T) {
		fillsQuery := `
			SELECT 
				e.timestamp,
				e.market_id,
				e.maker_id,
				e.side,
				e.size,
				e.price
			FROM enriched_order_filled_events e
			WHERE e.size::numeric > 100
			ORDER BY e.timestamp DESC
			LIMIT 5
		`
		rows, err := db.Query(ctx, fillsQuery)
		if err != nil {
			t.Fatalf("Whale Fills Query Failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var ts time.Time
			var mkt, whale, side, size, price string
			if err := rows.Scan(&ts, &mkt, &whale, &side, &size, &price); err != nil {
				t.Errorf("Scan failed: %v", err)
			}
			t.Logf("Fill: %s %s size=%s", side, ts, size)
		}
	})

	t.Run("ActivityByCategory", func(t *testing.T) {
		catQuery := `
			SELECT 
				COALESCE(pm.category, 'Unknown'),
				SUM(e.size::numeric * e.price::numeric) as vol,
				COUNT(*)
			FROM enriched_order_filled_events e
			LEFT JOIN plymkt_markets pm ON e.market_id = pm.id
			WHERE e.timestamp > NOW() - INTERVAL '24 hours'
			GROUP BY pm.category
			ORDER BY vol DESC
			LIMIT 5
		`
		rows, err := db.Query(ctx, catQuery)
		if err != nil {
			t.Fatalf("Category Query Failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var cat string
			var vol float64
			var count int
			if err := rows.Scan(&cat, &vol, &count); err != nil {
				t.Errorf("Scan failed: %v", err)
			}
			t.Logf("Category: %s, Vol: %.2f, Events: %d", cat, vol, count)
		}
	})
	t.Run("Leaderboard", func(t *testing.T) {
		// Query accounts directly to avoid view sync issues or complexity
		// Also ensure we are picking up valid numbers.
		query := `
			SELECT 
				id,
				COALESCE(scaled_collateral_volume::numeric, 0),
				COALESCE(scaled_profit::numeric, 0),
				COALESCE(num_trades::int, 0),
				last_traded_timestamp
			FROM accounts
			WHERE scaled_collateral_volume IS NOT NULL 
			  AND (scaled_collateral_volume::numeric) > 100
			ORDER BY scaled_collateral_volume::numeric DESC
			LIMIT 5
		`
		
		rows, err := db.Query(ctx, query)
		if err != nil {
			t.Fatalf("Leaderboard Query Failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id string
			var vol, profit float64
			var trades int
			var lastActive *time.Time
			
			// Scan must match SELECT columns exactly
			if err := rows.Scan(&id, &vol, &profit, &trades, &lastActive); err != nil {
				t.Errorf("Scan Error: %v", err)
				continue
			}
			t.Logf("Whale: %s, Vol: %.2f, Active: %v", id, vol, lastActive)
		}
	})
}
