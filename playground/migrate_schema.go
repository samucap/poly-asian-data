package main

import (
	"context"
	"fmt"
	"os"
    "github.com/joho/godotenv"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    _ = godotenv.Load()
	dbConnString := os.Getenv("POSTGRES_URL")
	if dbConnString == "" {
		user := os.Getenv("POSTGRES_USER")
		password := os.Getenv("POSTGRES_PASSWORD")
		host := os.Getenv("POSTGRES_HOST")
		port := os.Getenv("POSTGRES_PORT")
		dbName := os.Getenv("POSTGRES_DB")
		if host == "" { host = "localhost" }
		if port == "" { port = "5432" }
		if dbName == "" { dbName = "postgres" }	
		dbConnString = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
	}

	ctx := context.Background()
	dbPool, err := pgxpool.New(ctx, dbConnString)
	if err != nil {
		fmt.Printf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbPool.Close()

    queries := []string{
        `CREATE TABLE IF NOT EXISTS tags (
            id TEXT PRIMARY KEY,
            label TEXT,
            slug TEXT,
            force_show BOOLEAN,
            force_hide BOOLEAN,
            sport_id UUID,
            parent_tag_id TEXT,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );`,
        "ALTER TABLE hot_markets_vol ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'global';",
        "ALTER TABLE hot_markets_vol ADD COLUMN IF NOT EXISTS rank INTEGER;",
        "ALTER TABLE hot_markets_vol DROP CONSTRAINT IF EXISTS hot_markets_vol_pk_unique;", 
        "ALTER TABLE hot_markets_vol ADD CONSTRAINT hot_markets_vol_pk_unique UNIQUE (time, market_id, category);",
        "CREATE INDEX IF NOT EXISTS idx_hot_category ON hot_markets_vol (category, time DESC);",
    }

    for _, q := range queries {
        _, err := dbPool.Exec(ctx, q)
        if err != nil {
            fmt.Printf("Failed to execute: %s\nError: %v\n", q, err)
             // Check if constraint exists already error? (23505 duplicate key if constraint already executed? no, alter constraint creates it)
             // If constraint drop/add fails because of existing dupes, we might have issues.
             // But table is hypertable...
             // Let's assume it works or fails hard.
             os.Exit(1)
        }
        fmt.Println("Success: ", q)
    }
    fmt.Println("Migration complete.")
}
