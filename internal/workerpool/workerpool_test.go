package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Fan-In Tests
// =============================================================================

func TestFanIn(t *testing.T) {
	t.Run("merges multiple channels", func(t *testing.T) {
		ctx := context.Background()

		ch1 := make(chan int, 2)
		ch2 := make(chan int, 2)
		ch3 := make(chan int, 2)

		ch1 <- 1
		ch1 <- 2
		close(ch1)

		ch2 <- 3
		ch2 <- 4
		close(ch2)

		ch3 <- 5
		close(ch3)

		merged := FanIn(ctx, ch1, ch2, ch3)
		results := Collect(ctx, merged)

		assert.Len(t, results, 5)
		// All values should be present (order may vary due to concurrency)
		sum := 0
		for _, v := range results {
			sum += v
		}
		assert.Equal(t, 15, sum) // 1+2+3+4+5
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Use a buffered channel to avoid race
		ch := make(chan int, 10)
		for i := 0; i < 10; i++ {
			ch <- i
		}

		merged := FanIn(ctx, ch)

		// Read a few values
		<-merged
		<-merged
		<-merged

		// Cancel
		cancel()

		// Close source channel
		close(ch)

		// Give time for cleanup
		time.Sleep(time.Millisecond * 50)
	})

	t.Run("handles empty input", func(t *testing.T) {
		ctx := context.Background()
		merged := FanIn[int](ctx)

		results := Collect(ctx, merged)
		assert.Empty(t, results)
	})
}

// =============================================================================
// Fan-Out Tests
// =============================================================================

func TestFanOut(t *testing.T) {
	t.Run("distributes items round-robin", func(t *testing.T) {
		ctx := context.Background()

		input := make(chan int, 10)
		for i := 1; i <= 10; i++ {
			input <- i
		}
		close(input)

		outputs := FanOut(ctx, input, 3)
		assert.Len(t, outputs, 3)

		// Collect from each output
		var results [3][]int
		var wg sync.WaitGroup
		for i, out := range outputs {
			i, out := i, out
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = Collect(ctx, out)
			}()
		}
		wg.Wait()

		// Each should have some items
		total := len(results[0]) + len(results[1]) + len(results[2])
		assert.Equal(t, 10, total)

		// Sum should be correct
		sum := 0
		for _, r := range results {
			for _, v := range r {
				sum += v
			}
		}
		assert.Equal(t, 55, sum) // 1+2+...+10
	})

	t.Run("handles zero outputs", func(t *testing.T) {
		ctx := context.Background()
		input := make(chan int)
		close(input)

		outputs := FanOut(ctx, input, 0)
		assert.Len(t, outputs, 1) // Should default to 1
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		input := make(chan int, 10)
		for i := 0; i < 10; i++ {
			input <- i
		}

		outputs := FanOut(ctx, input, 2)

		// Read a couple values
		<-outputs[0]
		<-outputs[1]

		// Cancel context
		cancel()

		// Close input after cancel
		close(input)

		// Give time for goroutines to clean up
		time.Sleep(time.Millisecond * 50)
	})
}

// =============================================================================
// Pipeline Utility Tests
// =============================================================================

func TestGenerator(t *testing.T) {
	ctx := context.Background()

	ch := Generator(ctx, 1, 2, 3, 4, 5)
	results := Collect(ctx, ch)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, results)
}

func TestMap(t *testing.T) {
	ctx := context.Background()

	input := Generator(ctx, 1, 2, 3)
	doubled := Map(ctx, input, func(x int) int { return x * 2 })
	results := Collect(ctx, doubled)

	assert.Equal(t, []int{2, 4, 6}, results)
}

func TestFilter(t *testing.T) {
	ctx := context.Background()

	input := Generator(ctx, 1, 2, 3, 4, 5, 6)
	evens := Filter(ctx, input, func(x int) bool { return x%2 == 0 })
	results := Collect(ctx, evens)

	assert.Equal(t, []int{2, 4, 6}, results)
}

func TestBatch(t *testing.T) {
	t.Run("full batches", func(t *testing.T) {
		ctx := context.Background()

		input := Generator(ctx, 1, 2, 3, 4, 5, 6)
		batches := Batch(ctx, input, 2)
		results := Collect(ctx, batches)

		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5, 6}}, results)
	})

	t.Run("partial final batch", func(t *testing.T) {
		ctx := context.Background()

		input := Generator(ctx, 1, 2, 3, 4, 5)
		batches := Batch(ctx, input, 2)
		results := Collect(ctx, batches)

		assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, results)
	})

	t.Run("handles zero batch size", func(t *testing.T) {
		ctx := context.Background()

		input := Generator(ctx, 1, 2, 3)
		batches := Batch(ctx, input, 0)
		results := Collect(ctx, batches)

		// Should default to batch size 1
		assert.Len(t, results, 3)
	})
}

// =============================================================================
// Parallel Processing Tests
// =============================================================================

func TestParallel(t *testing.T) {
	t.Run("processes items concurrently", func(t *testing.T) {
		ctx := context.Background()

		input := Generator(ctx, 1, 2, 3, 4, 5)

		results := Parallel(ctx, input, 3, func(ctx context.Context, x int) (int, error) {
			time.Sleep(time.Millisecond * 10)
			return x * 2, nil
		})

		collected := Collect(ctx, results)
		assert.Len(t, collected, 5)

		// All should succeed
		for _, r := range collected {
			assert.NoError(t, r.Err)
		}
	})

	t.Run("handles errors in processor", func(t *testing.T) {
		ctx := context.Background()

		input := Generator(ctx, 1, 2, 3)

		results := Parallel(ctx, input, 2, func(ctx context.Context, x int) (int, error) {
			if x == 2 {
				return 0, errors.New("error on 2")
			}
			return x, nil
		})

		collected := Collect(ctx, results)
		assert.Len(t, collected, 3)

		errorCount := 0
		for _, r := range collected {
			if r.Err != nil {
				errorCount++
			}
		}
		assert.Equal(t, 1, errorCount)
	})

	t.Run("bounded concurrency", func(t *testing.T) {
		ctx := context.Background()

		input := Generator(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

		var concurrent atomic.Int32
		var maxConcurrent atomic.Int32

		results := Parallel(ctx, input, 3, func(ctx context.Context, x int) (int, error) {
			c := concurrent.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(time.Millisecond * 50)
			concurrent.Add(-1)
			return x, nil
		})

		Collect(ctx, results)

		assert.LessOrEqual(t, maxConcurrent.Load(), int32(3))
	})
}

// =============================================================================
// Error Group Tests
// =============================================================================

func TestErrorGroup(t *testing.T) {
	t.Run("collects errors", func(t *testing.T) {
		eg := &ErrorGroup{}

		eg.Add(errors.New("error 1"))
		eg.Add(nil) // Should be ignored
		eg.Add(errors.New("error 2"))

		assert.True(t, eg.HasErrors())
		assert.Len(t, eg.Errors(), 2)
		assert.Equal(t, 2, eg.Count())
	})

	t.Run("combined returns joined errors", func(t *testing.T) {
		eg := &ErrorGroup{}

		eg.Add(errors.New("error 1"))
		eg.Add(errors.New("error 2"))

		combined := eg.Combined()
		assert.Error(t, combined)
		assert.Contains(t, combined.Error(), "error 1")
		assert.Contains(t, combined.Error(), "error 2")
	})

	t.Run("combined returns nil when no errors", func(t *testing.T) {
		eg := &ErrorGroup{}
		assert.Nil(t, eg.Combined())
	})

	t.Run("thread safety", func(t *testing.T) {
		eg := &ErrorGroup{}
		var wg sync.WaitGroup

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				eg.Add(fmt.Errorf("error %d", i))
			}(i)
		}

		wg.Wait()
		assert.Len(t, eg.Errors(), 100)
	})
}

// =============================================================================
// Counter Tests
// =============================================================================

func TestCounter(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		c := &Counter{}

		assert.Equal(t, int64(0), c.Load())

		c.Inc()
		assert.Equal(t, int64(1), c.Load())

		c.Inc()
		c.Inc()
		assert.Equal(t, int64(3), c.Load())

		c.Dec()
		assert.Equal(t, int64(2), c.Load())

		c.Add(10)
		assert.Equal(t, int64(12), c.Load())

		c.Store(100)
		assert.Equal(t, int64(100), c.Load())

		prev := c.Reset()
		assert.Equal(t, int64(100), prev)
		assert.Equal(t, int64(0), c.Load())
	})

	t.Run("thread safety", func(t *testing.T) {
		c := &Counter{}
		var wg sync.WaitGroup

		// 100 goroutines each incrementing 100 times
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					c.Inc()
				}
			}()
		}

		wg.Wait()
		assert.Equal(t, int64(10000), c.Load())
	})
}

// =============================================================================
// Rate Limiter Tests
// =============================================================================

func TestRateLimiter(t *testing.T) {
	t.Run("creates rate limiter", func(t *testing.T) {
		rl := NewRateLimiter(10)
		require.NotNil(t, rl)
		rl.Stop()
	})

	t.Run("nil for zero rate", func(t *testing.T) {
		rl := NewRateLimiter(0)
		assert.Nil(t, rl)
	})

	t.Run("nil for negative rate", func(t *testing.T) {
		rl := NewRateLimiter(-1)
		assert.Nil(t, rl)
	})

	t.Run("try acquire works", func(t *testing.T) {
		rl := NewRateLimiter(5)
		defer rl.Stop()

		// Should be able to acquire up to 5 tokens
		for i := 0; i < 5; i++ {
			assert.True(t, rl.TryAcquire())
		}

		// 6th should fail
		assert.False(t, rl.TryAcquire())
	})

	t.Run("wait respects context cancellation", func(t *testing.T) {
		rl := NewRateLimiter(1)
		defer rl.Stop()

		// Drain the token
		rl.TryAcquire()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()

		err := rl.Wait(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})

	t.Run("nil rate limiter is safe", func(t *testing.T) {
		var rl *RateLimiter

		// These should not panic
		assert.True(t, rl.TryAcquire())
		assert.NoError(t, rl.Wait(context.Background()))
		rl.Stop() // Should not panic
	})
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkFanIn(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch1 := Generator(ctx, 1, 2, 3, 4, 5)
		ch2 := Generator(ctx, 6, 7, 8, 9, 10)
		ch3 := Generator(ctx, 11, 12, 13, 14, 15)

		merged := FanIn(ctx, ch1, ch2, ch3)
		Collect(ctx, merged)
	}
}

func BenchmarkFanOut(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input := Generator(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		outputs := FanOut(ctx, input, 3)

		var wg sync.WaitGroup
		for _, out := range outputs {
			out := out
			wg.Add(1)
			go func() {
				defer wg.Done()
				Collect(ctx, out)
			}()
		}
		wg.Wait()
	}
}

func BenchmarkParallel(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input := Generator(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		results := Parallel(ctx, input, 4, func(ctx context.Context, x int) (int, error) {
			return x * 2, nil
		})
		Collect(ctx, results)
	}
}

func BenchmarkCounter(b *testing.B) {
	c := &Counter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}
