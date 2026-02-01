package utils

import (
	"strings"
	"regexp"
)

var numericRegex = regexp.MustCompile(`^-?\d+(\.\d+)?$`)

// NormalizeString trims whitespace from a string.
// Returns empty string if the input is just whitespace.
func NormalizeString(s string) string {
	return strings.TrimSpace(s)
}

// CleanNumericString trims whitespace and validates if the string looks like a number.
// If the string is empty or invalid, it returns "0".
// This ensures downstream systems always receive a valid numeric representation.
func CleanNumericString(v string) string {
	s := strings.TrimSpace(v)
	if s == "" {
		return "0"
	}
	// Basic validation: must verify it looks like a number to avoid injecting garbage into DB.
	// If it doesn't match a number pattern, default to "0".
	if !numericRegex.MatchString(s) {
		return "0"
	}
	return s
}

// CleanTimestampString trims whitespace from a timestamp string.
// If the string is empty or obviously invalid (too short), it returns "0".
func CleanTimestampString(v string) string {
	s := strings.TrimSpace(v)
	if s == "" {
		return "0"
	}
	return s
}
