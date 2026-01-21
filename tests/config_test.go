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
	}{
		{
			name: "Success_Development",
			envVars: map[string]string{
				"SUBGRAPH_API_KEY": "subgraph_123",
				"POSTGRES_URL":     "postgres://user:pass@localhost:5432/db",
			},
			expectError: false,
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
				if tc.errorField != "" {
					validationErr, ok := err.(*config.ValidationError)
					if assert.True(t, ok, "Expected ValidationError, got %T", err) {
						assert.Equal(t, tc.errorField, validationErr.Field)
					}
				}
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, cfg.ENV)
				assert.NotEmpty(t, cfg.PostgresURL)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Run("has default endpoints", func(t *testing.T) {
		// Just verify DefaultEndpoints exists and has expected keys
		assert.NotNil(t, config.DefaultEndpoints)
		assert.NotNil(t, config.DefaultEndpoints["gamma"])
		assert.NotNil(t, config.DefaultEndpoints["clob"])
	})
}
