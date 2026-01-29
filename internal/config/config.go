package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"runtime"
	"strconv"
)

// DefaultEndpoints holds default API URLs.
var DefaultEndpoints = map[string]any{
	"gamma":    "https://gamma-api.polymarket.com",
	"clob":     "https://clob.polymarket.com",
	"subgraph": "https://api.thegraph.com/subgraphs", // Default placeholder
}

// Config holds the application configuration.
type Config struct {
	ENV            string
	LogLevel       string
	PostgresURL    string
	SubgraphAPIKey string
	Services       ServicesConfig

    // Worker Pool Configs
    FetcherCfg   WorkerPoolConfig
    ProcessorCfg WorkerPoolConfig
    SaverCfg     WorkerPoolConfig
}

type WorkerPoolConfig struct {
    NumWorkers int
    Qsize      int
}

type ServicesConfig struct {
	PlyMkt PlyMktConfig
}

type PlyMktConfig struct {
	Endpoints map[string]any
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	// Load .env file if it exists, but don't fail if it doesn't
	_ = godotenv.Load()

	// Dynamic defaults
	defaultWorkers := runtime.NumCPU() * 2 // Aggressive default
	if defaultWorkers < 4 {
		defaultWorkers = 4
	}

	cfg := &Config{
		ENV:            getEnv("ENV", "dev"),
		LogLevel:       getEnv("LOG_LEVEL", "info"),
		PostgresURL:    os.Getenv("POSTGRES_URL"),
		SubgraphAPIKey: os.Getenv("SUBGRAPH_API_KEY"),
		FetcherCfg: WorkerPoolConfig{
			NumWorkers: getEnvInt("FETCHER_WORKERS", defaultWorkers),
			Qsize:      getEnvInt("FETCHER_QUEUE_SIZE", 100),
		},
		ProcessorCfg: WorkerPoolConfig{
			NumWorkers: getEnvInt("PROCESSOR_WORKERS", defaultWorkers),
			Qsize:      getEnvInt("PROCESSOR_QUEUE_SIZE", 100),
		},
		SaverCfg: WorkerPoolConfig{
			NumWorkers: getEnvInt("SAVER_WORKERS", defaultWorkers/2+1), // Safer writes
			Qsize:      getEnvInt("SAVER_QUEUE_SIZE", 200),
		},
	}

	// Compose PostgresURL if not set
	if cfg.PostgresURL == "" {
		user := getEnv("POSTGRES_USER", "postgres")
		pass := getEnv("POSTGRES_PASSWORD", "postgres")
		host := getEnv("POSTGRES_HOST", "localhost")
		port := getEnv("POSTGRES_PORT", "5432")
		db := getEnv("POSTGRES_DB", "postgres") // Default db name often matches user or 'postgres'

		cfg.PostgresURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, db)
	}

	// Validate required fields
	if cfg.PostgresURL == "" {
		return nil, &ValidationError{Field: "PostgresURL", Message: "POSTGRES_URL is required or component vars"}
	}

	// Validate ENV
	if cfg.ENV != "dev" && cfg.ENV != "prod" && cfg.ENV != "test" {
		return nil, &ValidationError{Field: "ENV", Message: "ENV must be dev, prod, or test"}
	}

	// Initialize Services Config with defaults
	cfg.Services.PlyMkt.Endpoints = make(map[string]any)
	for k, v := range DefaultEndpoints {
		cfg.Services.PlyMkt.Endpoints[k] = v
	}

	return cfg, nil
}

func (c *Config) Validate() error {
    if c.PostgresURL == "" {
        return &ValidationError{Field: "PostgresURL", Message: "POSTGRES_URL is required"}
    }
    return nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}
