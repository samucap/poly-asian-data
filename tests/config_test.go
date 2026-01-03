package config_test

import (
	"context"
	"os"
	"testing"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	// Setup: Clear environment variables before each test
	clearEnv := func() {
		os.Unsetenv("POLYMARKET_API_KEY")
		os.Unsetenv("ODDS_API_KEY")
		os.Unsetenv("POSTGRES_URL")
		os.Unsetenv("ENV")
		os.Unsetenv("LOG_LEVEL")
	}

	tests := []struct {
		name        string
		envVars     map[string]string
		expectError bool
		errorField  string
		expectedCfg *config.Config
	}{
		{
			name: "Success_Development",
			envVars: map[string]string{
				"POLYMARKET_API_KEY": "poly_123",
				"ODDS_API_KEY":       "odds_123",
				"POSTGRES_URL":       "postgres://user:pass@localhost:5432/db",
			},
			expectError: false,
			expectedCfg: &config.Config{
				PolymarketAPIKey: "poly_123",
				OddsAPIKey:       "odds_123",
				PostgresURL:      "postgres://user:pass@localhost:5432/db",
				Environment:      "development",
				LogLevel:         "info",
			},
		},
		{
			name: "Success_Production",
			envVars: map[string]string{
				"POLYMARKET_API_KEY": "poly_123",
				"ODDS_API_KEY":       "odds_123",
				"POSTGRES_URL":       "postgres://localhost:5432/db",
				"ENV":                "production",
				"LOG_LEVEL":          "warn",
			},
			expectError: false,
			expectedCfg: &config.Config{
				PolymarketAPIKey: "poly_123",
				OddsAPIKey:       "odds_123",
				PostgresURL:      "postgres://localhost:5432/db",
				Environment:      "production",
				LogLevel:         "warn",
			},
		},
		{
			name: "Missing_PolymarketKey",
			envVars: map[string]string{
				"ODDS_API_KEY": "odds_123",
				"POSTGRES_URL": "postgres://localhost:5432/db",
			},
			expectError: true,
			errorField:  "PolymarketAPIKey",
		},
		{
			name: "Missing_OddsKey",
			envVars: map[string]string{
				"POLYMARKET_API_KEY": "poly_123",
				"POSTGRES_URL":       "postgres://localhost:5432/db",
			},
			expectError: true,
			errorField:  "OddsAPIKey",
		},
		{
			name: "Missing_PostgresURL",
			envVars: map[string]string{
				"POLYMARKET_API_KEY": "poly_123",
				"ODDS_API_KEY":       "odds_123",
			},
			expectError: true,
			errorField:  "PostgresURL",
		},
		{
			name: "Invalid_Environment",
			envVars: map[string]string{
				"POLYMARKET_API_KEY": "poly_123",
				"ODDS_API_KEY":       "odds_123",
				"POSTGRES_URL":       "postgres://localhost:5432/db",
				"ENV":                "invalid_env",
			},
			expectError: true,
			errorField:  "Environment",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv()
			for k, v := range tc.envVars {
				os.Setenv(k, v)
			}
			defer clearEnv()

			cfg, err := config.Load(context.Background())

			if tc.expectError {
				require.Error(t, err)
				validationErr, ok := err.(*config.ValidationError)
				if assert.True(t, ok, "Expected ValidationError, got %T", err) {
					assert.Equal(t, tc.errorField, validationErr.Field)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedCfg, cfg)
			}
		})
	}
}
