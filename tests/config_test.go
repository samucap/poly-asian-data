package config_test

import (
	"os"
	"testing"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	// Setup: Clear environment variables before each test
	clearEnv := func() {
		os.Unsetenv("SUBGRAPH_API_KEY")
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
				"SUBGRAPH_API_KEY": "subgraph_123",
				"POSTGRES_URL":     "postgres://user:pass@localhost:5432/db",
			},
			expectError: false,
			expectedCfg: &config.Config{
				SubgraphAPIKey: "subgraph_123",
				PostgresURL:    "postgres://user:pass@localhost:5432/db",
				ENV:            "dev",
				LogLevel:       "debug",
			},
		},
		{
			name: "Success_Production",
			envVars: map[string]string{
				"SUBGRAPH_API_KEY": "subgraph_123",
				"POSTGRES_URL":     "postgres://localhost:5432/db",
				"ENV":              "prod",
				"LOG_LEVEL":        "warn",
			},
			expectError: false,
			expectedCfg: &config.Config{
				SubgraphAPIKey: "subgraph_123",
				PostgresURL:    "postgres://localhost:5432/db",
				ENV:            "prod",
				LogLevel:       "warn",
			},
		},
		{
			name: "Missing_SubgraphKey",
			envVars: map[string]string{
				"POSTGRES_URL": "postgres://localhost:5432/db",
			},
			expectError: true,
			errorField:  "SubgraphAPIKey",
		},
		{
			name: "Missing_PostgresURL",
			envVars: map[string]string{
				"SUBGRAPH_API_KEY": "subgraph_123",
			},
			expectError: true,
			errorField:  "PostgresURL",
		},
		{
			name: "Invalid_Environment",
			envVars: map[string]string{
				"SUBGRAPH_API_KEY": "subgraph_123",
				"POSTGRES_URL":     "postgres://localhost:5432/db",
				"ENV":              "invalid_env",
			},
			expectError: true,
			errorField:  "ENV",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv()
			for k, v := range tc.envVars {
				os.Setenv(k, v)
			}
			defer clearEnv()

			cfg, err := config.Load()

			if tc.expectError {
				require.Error(t, err)
				validationErr, ok := err.(*config.ValidationError)
				if assert.True(t, ok, "Expected ValidationError, got %T", err) {
					assert.Equal(t, tc.errorField, validationErr.Field)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedCfg.ENV, cfg.ENV)
				assert.Equal(t, tc.expectedCfg.SubgraphAPIKey, cfg.SubgraphAPIKey)
				assert.Equal(t, tc.expectedCfg.PostgresURL, cfg.PostgresURL)
			}
		})
	}
}
