package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/samucap/poly-asian-data/internal/config"
)

func main() {
	ctx := context.Background()
	
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	conn, err := pgx.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "DROP VIEW IF EXISTS whale_candidates CASCADE;")
	if err != nil {
		log.Fatalf("Failed to drop view: %v", err)
	}

	log.Println("Successfully dropped whale_candidates view. Restart whale-sync to recreate with new schema.")
}
