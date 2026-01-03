package config

import "fmt"

// ValidationError represents an error that occurs during configuration validation.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: %s - %s", e.Field, e.Message)
}

// ConfigError wraps errors that occur during configuration loading.
type ConfigError struct {
	Op  string
	Err error
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config %s: %v", e.Op, e.Err)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}
