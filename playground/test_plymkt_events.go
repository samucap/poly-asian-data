package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/samucap/poly-asian-data/internal/services"
)

// upsertPlyMktEvents inserts/updates events into the plymkt_events table
func upsertPlyMktEvents(ctx context.Context, db *pgxpool.Pool, events []*services.PlyMktEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	sql := `
		INSERT INTO plymkt_events (
			id, ticker, slug, title, description, start_date, end_date, category,
			image, icon, active, closed, archived, new, featured, restricted,
			liquidity, volume, volume_24hr, volume_1wk, volume_1mo, volume_1yr,
			liquidity_clob, competitive, neg_risk, neg_risk_market_id, comment_count,
			enable_order_book, series_slug, live, ended, creator_id,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22,
			$23, $24, $25, $26, $27,
			$28, $29, $30, $31, $32,
			$33, $34
		)
		ON CONFLICT (id) DO UPDATE SET
			ticker = EXCLUDED.ticker,
			slug = EXCLUDED.slug,
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			start_date = EXCLUDED.start_date,
			end_date = EXCLUDED.end_date,
			category = EXCLUDED.category,
			image = EXCLUDED.image,
			icon = EXCLUDED.icon,
			active = EXCLUDED.active,
			closed = EXCLUDED.closed,
			archived = EXCLUDED.archived,
			new = EXCLUDED.new,
			featured = EXCLUDED.featured,
			restricted = EXCLUDED.restricted,
			liquidity = EXCLUDED.liquidity,
			volume = EXCLUDED.volume,
			volume_24hr = EXCLUDED.volume_24hr,
			volume_1wk = EXCLUDED.volume_1wk,
			volume_1mo = EXCLUDED.volume_1mo,
			volume_1yr = EXCLUDED.volume_1yr,
			liquidity_clob = EXCLUDED.liquidity_clob,
			competitive = EXCLUDED.competitive,
			neg_risk = EXCLUDED.neg_risk,
			neg_risk_market_id = EXCLUDED.neg_risk_market_id,
			comment_count = EXCLUDED.comment_count,
			enable_order_book = EXCLUDED.enable_order_book,
			series_slug = EXCLUDED.series_slug,
			live = EXCLUDED.live,
			ended = EXCLUDED.ended,
			creator_id = EXCLUDED.creator_id,
			updated_at = EXCLUDED.updated_at;
	`

	for _, e := range events {
		batch.Queue(sql,
			e.ID,              // id
			e.Ticker,          // ticker
			e.Slug,            // slug
			e.Title,           // title
			e.Description,     // description
			e.StartDate,       // start_date
			e.EndDate,         // end_date
			e.Category,        // category
			e.Image,           // image
			e.Icon,            // icon
			e.Active,          // active
			e.Closed,          // closed
			e.Archived,        // archived
			e.New,             // new
			e.Featured,        // featured
			e.Restricted,      // restricted
			e.Liquidity,       // liquidity
			e.Volume,          // volume
			e.Volume24hr,      // volume_24hr
			e.Volume1wk,       // volume_1wk
			e.Volume1mo,       // volume_1mo
			e.Volume1yr,       // volume_1yr
			e.LiquidityClob,   // liquidity_clob
			e.Competitive,     // competitive
			e.NegRisk,         // neg_risk
			e.NegRiskMarketID, // neg_risk_market_id
			e.CommentCount,    // comment_count
			e.EnableOrderBook, // enable_order_book
			e.SeriesSlug,      // series_slug
			false,             // live (default)
			false,             // ended (default)
			e.CreatedBy,       // creator_id
			e.CreatedAt,       // created_at
			time.Now(),        // updated_at
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(events); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("error executing batch item %d: %w", i, err)
		}
	}

	fmt.Printf("Upserted %d plymkt events\n", len(events))
	return nil
}

func main() {
	_ = godotenv.Load()
	dbConnString := os.Getenv("POSTGRES_URL")
	if dbConnString == "" {
		user := os.Getenv("POSTGRES_USER")
		password := os.Getenv("POSTGRES_PASSWORD")
		host := os.Getenv("POSTGRES_HOST")
		port := os.Getenv("POSTGRES_PORT")
		dbName := os.Getenv("POSTGRES_DB")
		if host == "" {
			host = "localhost"
		}
		if port == "" {
			port = "5432"
		}
		if dbName == "" {
			dbName = "postgres"
		}
		dbConnString = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
	}

	ctx := context.Background()
	dbPool, err := pgxpool.New(ctx, dbConnString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbPool.Close()

	// Create a test event
	testEvent := &services.PlyMktEvent{
		ID:              "test-event-123",
		Ticker:          "TEST",
		Slug:            "test-event",
		Title:           "Test Event",
		Description:     "A test event for validation",
		StartDate:       time.Now(),
		EndDate:         time.Now().Add(24 * time.Hour),
		Category:        "test",
		Image:           "test.jpg",
		Icon:            "test.ico",
		Active:          true,
		Closed:          false,
		Archived:        false,
		New:             true,
		Featured:        false,
		Restricted:      false,
		Liquidity:       1000.0,
		Volume:          500.0,
		Volume24hr:      100.0,
		Volume1wk:       200.0,
		Volume1mo:       300.0,
		Volume1yr:       400.0,
		LiquidityClob:   800.0,
		Competitive:     0.5,
		NegRisk:         false,
		NegRiskMarketID: "",
		CommentCount:    5,
		EnableOrderBook: true,
		SeriesSlug:      "test-series",
		CreatedBy:       "test-user",
		CreatedAt:       time.Now(),
	}

	// Test upsert
	events := []*services.PlyMktEvent{testEvent}
	err = upsertPlyMktEvents(ctx, dbPool, events)
	if err != nil {
		log.Fatalf("Failed to upsert test event: %v", err)
	}

	fmt.Println("✅ Successfully upserted test plymkt event")

	// Verify it was inserted
	var count int
	err = dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM plymkt_events WHERE id = 'test-event-123'").Scan(&count)
	if err != nil {
		log.Fatalf("Failed to verify insertion: %v", err)
	}

	if count == 1 {
		fmt.Println("✅ Test event found in database")
	} else {
		fmt.Printf("❌ Test event not found in database (count: %d)\n", count)
	}

	// Clean up
	_, err = dbPool.Exec(ctx, "DELETE FROM plymkt_events WHERE id = 'test-event-123'")
	if err != nil {
		log.Printf("Failed to clean up test data: %v", err)
	} else {
		fmt.Println("✅ Cleaned up test data")
	}
}
