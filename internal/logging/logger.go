package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// sensitivePatterns are patterns to redact (case-insensitive).
var sensitivePatterns = []string{
	"password",
	"key",
	"secret",
	"token",
	"auth",
	"credential",
	"apikey",
	"api_key",
}

// redactedValue is the placeholder for sensitive data.
const redactedValue = "[REDACTED]"

// Logger is the global structured logger.
var Logger *slog.Logger

// Init initializes the global logger with redaction support.
// In non-production, source location is enabled.
func Init(env string) {
	isProduction := env == "prod"

	opts := &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: !isProduction, // Only add source in non-production
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Check if this attribute's key matches a sensitive pattern
			keyLower := strings.ToLower(a.Key)
			for _, pattern := range sensitivePatterns {
				if strings.Contains(keyLower, pattern) {
					return slog.String(a.Key, redactedValue)
				}
			}
			return a
		},
	}

	var handler slog.Handler
	if isProduction {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = NewPrettyHandler(os.Stdout, opts)
	}

	Logger = slog.New(handler)
	slog.SetDefault(Logger)
}

// WithContext returns a logger with context values (for request tracing, etc.).
func WithContext(ctx context.Context) *slog.Logger {
	// Future: Extract trace IDs from context here
	return Logger
}

// Info logs at INFO level.
func Info(msg string, args ...any) {
	Logger.Info(msg, args...)
}

// Error logs at ERROR level.
func Error(msg string, args ...any) {
	Logger.Error(msg, args...)
}

// Debug logs at DEBUG level.
func Debug(msg string, args ...any) {
	Logger.Debug(msg, args...)
}

// Warn logs at WARN level.
func Warn(msg string, args ...any) {
	Logger.Warn(msg, args...)
}
