package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"
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
// Level defaults to info; override with LOG_LEVEL=debug|info|warn|error.
func Init(env string) {
	InitWithLevel(env, levelFromEnv())
}

// InitWithLevel initializes the global logger at an explicit level.
func InitWithLevel(env string, level slog.Level) {
	isProduction := env == "prod"

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: !isProduction,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Value.Kind() == slog.KindDuration {
				return slog.String(a.Key, FormatDuration(a.Value.Duration()))
			}
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

func levelFromEnv() slog.Level {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func get() *slog.Logger {
	if Logger != nil {
		return Logger
	}
	return slog.Default()
}

// With returns a child logger with attributes (nil-safe).
func With(args ...any) *slog.Logger {
	return get().With(args...)
}

// WithContext returns a logger with context values (for request tracing, etc.).
func WithContext(ctx context.Context) *slog.Logger {
	_ = ctx
	return get()
}

// Info logs at INFO level.
func Info(msg string, args ...any) {
	get().Info(msg, args...)
}

// Error logs at ERROR level.
func Error(msg string, args ...any) {
	get().Error(msg, args...)
}

// Debug logs at DEBUG level.
func Debug(msg string, args ...any) {
	get().Debug(msg, args...)
}

// Warn logs at WARN level.
func Warn(msg string, args ...any) {
	get().Warn(msg, args...)
}

// FormatCount renders large integers as compact human form (1.2k, 3.4M, 1.1B).
func FormatCount(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	var s string
	switch {
	case n >= 1_000_000_000:
		s = trimFloat(float64(n)/1e9) + "B"
	case n >= 1_000_000:
		s = trimFloat(float64(n)/1e6) + "M"
	case n >= 1_000:
		s = trimFloat(float64(n)/1e3) + "k"
	default:
		s = itoa(n)
	}
	if neg {
		return "-" + s
	}
	return s
}

// FormatFloat compactly formats monetary/volume figures (450, 1.2k, 4.2M).
func FormatFloat(f float64) string {
	neg := f < 0
	if neg {
		f = -f
	}
	var s string
	switch {
	case f >= 1_000_000_000:
		s = trimFloat(f/1e9) + "B"
	case f >= 1_000_000:
		s = trimFloat(f/1e6) + "M"
	case f >= 1_000:
		s = trimFloat(f/1e3) + "k"
	default:
		s = trimFloat(f)
	}
	if neg {
		return "-" + s
	}
	return s
}

// FormatBytes renders byte counts (1.4KB, 2.3MB).
func FormatBytes(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	var s string
	switch {
	case n >= 1<<30:
		s = trimFloat(float64(n)/float64(1<<30)) + "GB"
	case n >= 1<<20:
		s = trimFloat(float64(n)/float64(1<<20)) + "MB"
	case n >= 1<<10:
		s = trimFloat(float64(n)/float64(1<<10)) + "KB"
	default:
		s = itoa(n) + "B"
	}
	if neg {
		return "-" + s
	}
	return s
}

// FormatRate renders a success rate as a percentage from ok/fail counts (e.g. "99.5%").
// When there were no finished ops, returns "n/a".
func FormatRate(ok, failed int64) string {
	total := ok + failed
	if total <= 0 {
		return "n/a"
	}
	if failed <= 0 {
		return "100%"
	}
	if ok <= 0 {
		return "0%"
	}
	// One decimal for non-round rates; whole percent when exact.
	pct := float64(ok) / float64(total) * 100
	if pct >= 99.95 {
		return "100%"
	}
	x := int64(pct*10 + 0.5)
	whole := x / 10
	frac := x % 10
	if frac == 0 {
		return itoa(whole) + "%"
	}
	return itoa(whole) + "." + itoa(frac) + "%"
}

// FormatDuration renders durations without raw nanoseconds (45ms, 1.2s, 3m12s).
func FormatDuration(d time.Duration) string {
	if d < 0 {
		return "-" + FormatDuration(-d)
	}
	if d == 0 {
		return "0s"
	}
	if d < time.Millisecond {
		return itoa(int64(d.Microseconds())) + "µs"
	}
	if d < time.Second {
		return itoa(int64(d/time.Millisecond)) + "ms"
	}
	if d < time.Minute {
		sec := float64(d) / float64(time.Second)
		return trimFloat(sec) + "s"
	}
	if d < time.Hour {
		m := d / time.Minute
		s := (d % time.Minute) / time.Second
		if s == 0 {
			return itoa(int64(m)) + "m"
		}
		return itoa(int64(m)) + "m" + itoa(int64(s)) + "s"
	}
	h := d / time.Hour
	m := (d % time.Hour) / time.Minute
	if m == 0 {
		return itoa(int64(h)) + "h"
	}
	return itoa(int64(h)) + "h" + itoa(int64(m)) + "m"
}

func trimFloat(f float64) string {
	if f < 0 {
		return "-" + trimFloat(-f)
	}
	// Round to one decimal for small magnitudes; whole for large.
	if f >= 100 {
		return itoa(int64(f + 0.5))
	}
	x := int64(f*10 + 0.5)
	whole := x / 10
	frac := x % 10
	if frac == 0 {
		return itoa(whole)
	}
	return itoa(whole) + "." + itoa(frac)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
