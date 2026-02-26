package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func dbChecker() {
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

	fmt.Printf("Connected to database: %s\n", dbConnString)

	// Check if tags table exists
	var exists bool
	err = dbPool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'tags')").Scan(&exists)
	if err != nil {
		log.Fatalf("Failed to check table existence: %v", err)
	}

	if exists {
		fmt.Println("✅ tags table exists")
	} else {
		fmt.Println("❌ tags table does NOT exist")
	}

	// Check if hot_events table exists
	err = dbPool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'hot_events')").Scan(&exists)
	if err != nil {
		log.Printf("Failed to check hot_events table: %v", err)
	} else if exists {
		fmt.Println("✅ hot_events table exists")
	} else {
		fmt.Println("❌ hot_events table does NOT exist")
	}

	// Check if hot_markets_vol table exists
	err = dbPool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'hot_markets_vol')").Scan(&exists)
	if err != nil {
		log.Printf("Failed to check hot_markets_vol table: %v", err)
	} else if exists {
		fmt.Println("✅ hot_markets_vol table exists")
	} else {
		fmt.Println("❌ hot_markets_vol table does NOT exist")
	}

	// Show all tables
	rows, err := dbPool.Query(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
	if err != nil {
		log.Printf("Failed to list tables: %v", err)
	} else {
		fmt.Println("\nAll tables in public schema:")
		for rows.Next() {
			var tableName string
			rows.Scan(&tableName)
			fmt.Printf("  - %s\n", tableName)
		}
		rows.Close()
	}
}
