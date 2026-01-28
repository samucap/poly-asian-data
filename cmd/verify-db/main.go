package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	counts := map[string]string{
		"sports":  "SELECT count(*) FROM sports",
		"leagues": "SELECT count(*) FROM leagues",
		"teams":   "SELECT count(*) FROM teams",
		"tags_linked": "SELECT count(*) FROM tags WHERE sport_id IS NOT NULL",
		"teams_linked": "SELECT count(*) FROM teams WHERE sport_id IS NOT NULL",
	}

	for name, query := range counts {
		var count int
		if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
			fmt.Printf("%s: error %v\n", name, err)
		} else {
			fmt.Printf("%s: %d\n", name, count)
		}
	}
}
