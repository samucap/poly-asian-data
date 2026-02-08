package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
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

	// Test a simple insert
	_, err = dbPool.Exec(ctx, `
		INSERT INTO tags (id, label, slug, force_show, force_hide, parent_tag_id, created_at, updated_at)
		VALUES ('test-123', 'Test Label', 'test-slug', false, false, NULL, NOW(), NOW())
		ON CONFLICT (id) DO UPDATE SET
			label = EXCLUDED.label,
			updated_at = EXCLUDED.updated_at
	`)

	if err != nil {
		log.Printf("Test insert failed: %v", err)
	} else {
		fmt.Println("Test insert succeeded")
	}

	// Check count again
	var count int
	dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM tags").Scan(&count)
	fmt.Printf("Total tags after test insert: %d\n", count)

	// Clean up test data
	dbPool.Exec(ctx, "DELETE FROM tags WHERE id = 'test-123'")
}