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

	fmt.Printf("Using database connection: %s\n", dbConnString)

	ctx := context.Background()
	dbPool, err := pgxpool.New(ctx, dbConnString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbPool.Close()

	// Check which schema the tags table is in
	var schemaName string
	err = dbPool.QueryRow(ctx, "SELECT schemaname FROM pg_tables WHERE tablename = 'tags'").Scan(&schemaName)
	if err != nil {
		log.Printf("Failed to find tags table schema: %v", err)
	} else {
		fmt.Printf("Tags table is in schema: %s\n", schemaName)
	}

	// Check tag count
	var count int
	err = dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM tags").Scan(&count)
	if err != nil {
		log.Fatalf("Failed to query tags: %v", err)
	}

	fmt.Printf("Total tags in database: %d\n", count)

	// Show a few examples
	rows, err := dbPool.Query(ctx, "SELECT id, label, slug, parent_tag_id FROM tags LIMIT 10")
	if err != nil {
		log.Fatalf("Failed to query tags: %v", err)
	}
	defer rows.Close()

	fmt.Println("\nSample tags:")
	fmt.Println("ID\tLabel\tSlug\tParent")
	for rows.Next() {
		var id, label, slug string
		var parent *string
		err := rows.Scan(&id, &label, &slug, &parent)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}
		parentStr := "NULL"
		if parent != nil {
			parentStr = *parent
		}
		fmt.Printf("%s\t%s\t%s\t%s\n", id, label, slug, parentStr)
	}
}