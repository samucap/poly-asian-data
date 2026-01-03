package config

import (
	"context"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// Config holds the application configuration with strict typing.
// Load order: System ENV > .env file > Defaults.
type Config struct {
	// Environment: "development", "staging", "production"
	Environment string `mapstructure:"ENV" validate:"required,oneof=development staging production"`

	// API Keys
	PolymarketAPIKey string `mapstructure:"POLYMARKET_API_KEY" validate:"required"`
	OddsAPIKey       string `mapstructure:"ODDS_API_KEY" validate:"required"`

	// Database
	PostgresURL string `mapstructure:"POSTGRES_URL" validate:"required,url"`

	// Logging
	LogLevel string `mapstructure:"LOG_LEVEL" validate:"omitempty,oneof=debug info warn error"`
}

// validate is the singleton validator instance.
var validate = validator.New()

// Load reads configuration from environment variables and .env file.
// Priority: System ENV > .env file > Defaults.
func Load(ctx context.Context) (*Config, error) {
	// Step 1: Load .env file (does NOT overwrite existing system env vars)
	// Ignore error if .env doesn't exist (common in Docker/CI)
	_ = godotenv.Load()

	// Step 2: Initialize Viper
	v := viper.New()

	// Set defaults
	v.SetDefault("ENV", "development")
	v.SetDefault("LOG_LEVEL", "info")

	// Step 3: Bind to environment variables (this gives system ENV priority)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Explicitly bind keys we care about
	keys := []string{"ENV", "POLYMARKET_API_KEY", "ODDS_API_KEY", "POSTGRES_URL", "LOG_LEVEL"}
	for _, key := range keys {
		if err := v.BindEnv(key); err != nil {
			return nil, &ConfigError{Op: "bind_env", Err: err}
		}
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
	return c.Environment == "production"
}
