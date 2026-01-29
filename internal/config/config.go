package config

import (
	"runtime"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// DefaultEndpoints contains the default Polymarket API endpoints.
var DefaultEndpoints = map[string]any{
	"gamma": "https://gamma-api.polymarket.com",      // Market discovery, metadata, events
	"clob":  "https://clob.polymarket.com",           // Order management, prices, orderbooks
	"data":  "https://data-api.polymarket.com",       // User positions, activity, history
	"webSockets": map[string]string{
		"sports": "wss://sports-api.polymarket.com/ws",
		"clob":   "wss://ws-subscriptions-clob.polymarket.com/ws", // Orderbook updates, order status
		"rtds":   "wss://ws-live-data.polymarket.com",             // Low-latency crypto prices, comments
	},
	"subgraph": "https://gateway.thegraph.com/api/subgraphs/id",
}

// Config holds the application configuration with strict typing.
// Load order: System ENV > .env file > Defaults.

type PlyMktSvc struct {
	Endpoints      map[string]any `mapstructure:"ENDPOINTS" validate:"required"`
	SubgraphAPIKey string `mapstructure:"SUBGRAPH_API_KEY" validate:"required"`
	MaxRetries int
	RetryDelay time.Duration
}

type SvcProvider struct {
	PlyMkt *PlyMktSvc
}

type PoolCfg struct {
	NumWorkers int
	Qsize      int
}

type SaverCfg struct {
	NumWorkers int
	Qsize      int
}

type Config struct {
	// Environment: "development", "staging", "production"
	ENV string `mapstructure:"ENV" validate:"required,oneof=dev stg prod"`

	// API Keys
	//PolymarketAPIKey string `mapstructure:"POLYMARKET_API_KEY" validate:"required"`
	// OddsAPIKey       string `mapstructure:"ODDS_API_KEY" validate:"required"`

	// Database
	PostgresURL string `mapstructure:"POSTGRES_URL" validate:"required,url"`

	// Logging
	LogLevel string `mapstructure:"LOG_LEVEL" validate:"omitempty,oneof=debug info warn error"`
	Services SvcProvider

	// Pipeline
	SaverCfg     SaverCfg
	FetcherCfg   PoolCfg
	ProcessorCfg PoolCfg
	SubgraphAPIKey string `mapstructure:"SUBGRAPH_API_KEY" validate:"omitempty"` // Optional or required? User has it in .env
}

// validate is the singleton validator instance.
var validate = validator.New()

// Load reads configuration from environment variables and .env file.
// Priority: System ENV > .env file > Defaults.
func Load() (*Config, error) {
	// Step 1: Load .env file (does NOT overwrite existing system env vars)
	// Ignore error if .env doesn't exist (common in Docker/CI)
	_ = godotenv.Load()

	// Step 2: Initialize Viper
	v := viper.New()

	// Set defaults
	v.SetDefault("ENV", "dev")
	v.SetDefault("LOG_LEVEL", "debug")
	v.SetDefault("Services.PlyMkt.Endpoints", DefaultEndpoints)
	v.SetDefault("Services.PlyMkt.MaxRetries", 3)
	v.SetDefault("Services.PlyMkt.RetryDelay", 1*time.Second)
	// Calculate optimal defaults based on system resources
	numCPU := runtime.NumCPU()

	// Fetcher: I/O bound (HTTP requests). Can handle high concurrency.
	// Recommended: 4x Cores. Min: 4.
	fetcherWorkers := numCPU * 4
	if fetcherWorkers < 4 {
		fetcherWorkers = 4
	}

	// Processor: CPU bound (JSON parsing, logic).
	// Recommended: 1x Cores. Min: 2.
	processorWorkers := numCPU
	if processorWorkers < 2 {
		processorWorkers = 2
	}

	// Saver: I/O bound (Database writes), but limited by connection pool often.
	// Recommended: 2x Cores. Min: 2.
	saverWorkers := numCPU * 2
	if saverWorkers < 2 {
		saverWorkers = 2
	}

	v.SetDefault("FetcherCfg.NumWorkers", fetcherWorkers)
	v.SetDefault("FetcherCfg.Qsize", fetcherWorkers*5) // Queue buffer 5x workers
	v.SetDefault("ProcessorCfg.NumWorkers", processorWorkers)
	v.SetDefault("ProcessorCfg.Qsize", processorWorkers*5)
	v.SetDefault("SaverCfg.NumWorkers", saverWorkers)
	v.SetDefault("SaverCfg.Qsize", saverWorkers*5)

	// Step 3: Bind to environment variables (this gives system ENV priority)
	//v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Explicitly bind keys we care about
	keys := []string{"ENV", "SUBGRAPH_API_KEY", "ODDS_API_KEY", "POSTGRES_URL", "LOG_LEVEL"}
	for _, key := range keys {
		if err := v.BindEnv(key); err != nil {
			return nil, &ConfigError{Op: "bind_env", Err: err}
		}
	}
    // Bind nested keys
    if err := v.BindEnv("Services.PlyMkt.SUBGRAPH_API_KEY", "SUBGRAPH_API_KEY"); err != nil {
        return nil, &ConfigError{Op: "bind_env_nested", Err: err}
    }

	// Step 4: Unmarshal into struct
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, &ConfigError{Op: "unmarshal", Err: err}
	}

	// Step 5: Validate
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks all required fields and constraints.
func (c *Config) Validate() error {
	if err := validate.Struct(c); err != nil {
		// Extract first validation error for clarity
		if validationErrors, ok := err.(validator.ValidationErrors); ok && len(validationErrors) > 0 {
			first := validationErrors[0]
			return &ValidationError{
				Field:   first.Field(),
				Message: formatValidationError(first),
			}
		}
		return &ConfigError{Op: "validate", Err: err}
	}
	return nil
}

// formatValidationError returns a human-readable message for a validation error.
func formatValidationError(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "url":
		return "must be a valid URL"
	case "oneof":
		return "must be one of: " + fe.Param()
	default:
		return "failed validation: " + fe.Tag()
	}
}

// IsProduction returns true if running in production environment.
func (c *Config) IsProduction() bool {
	return c.ENV == "prod"
}
