package logging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatCount(t *testing.T) {
	assert.Equal(t, "0", FormatCount(0))
	assert.Equal(t, "999", FormatCount(999))
	assert.Equal(t, "1.2k", FormatCount(1234))
	assert.Equal(t, "1.5M", FormatCount(1_500_000))
	assert.Equal(t, "2B", FormatCount(2_000_000_000))
}

func TestFormatDuration(t *testing.T) {
	assert.Equal(t, "0s", FormatDuration(0))
	assert.Equal(t, "45ms", FormatDuration(45*time.Millisecond))
	assert.Equal(t, "1.5s", FormatDuration(1500*time.Millisecond))
	assert.Equal(t, "3m12s", FormatDuration(3*time.Minute+12*time.Second))
	assert.Equal(t, "1h5m", FormatDuration(time.Hour+5*time.Minute))
}

func TestFormatFloat(t *testing.T) {
	assert.Equal(t, "1.2k", FormatFloat(1234))
	assert.Equal(t, "4.2M", FormatFloat(4_200_000))
}

func TestFormatRate(t *testing.T) {
	assert.Equal(t, "n/a", FormatRate(0, 0))
	assert.Equal(t, "100%", FormatRate(10, 0))
	assert.Equal(t, "0%", FormatRate(0, 5))
	assert.Equal(t, "80%", FormatRate(8, 2))
	assert.Equal(t, "99.5%", FormatRate(199, 1))
}
