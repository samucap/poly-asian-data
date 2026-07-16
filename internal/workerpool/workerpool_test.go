package workerpool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.Default()
}

func TestNewPool_Validation(t *testing.T) {
	ctx := context.Background()
	_, err := NewPool[int, int](ctx, "t", 0, 10, testLogger(), func(context.Context, int) (int, error) { return 0, nil })
	assert.Error(t, err)

	_, err = NewPool[int, int](ctx, "t", 2, 0, testLogger(), func(context.Context, int) (int, error) { return 0, nil })
	assert.Error(t, err)

	_, err = NewPool[int, int](ctx, "t", 2, 10, testLogger(), nil)
	assert.Error(t, err)
}

func TestPool_ProcessesItems(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool[int, int](ctx, "t", 2, 16, testLogger(), func(_ context.Context, x int) (int, error) {
		return x * 2, nil
	})
	require.NoError(t, err)
	defer p.Stop()

	const n = 20
	for i := 0; i < n; i++ {
		require.NoError(t, p.SubmitWait(ctx, i))
	}

	// Collect results
	got := make(map[int]bool)
	deadline := time.After(2 * time.Second)
	for len(got) < n {
		select {
		case r := <-p.Outputs():
			require.NoError(t, r.Err)
			got[r.Value] = true
		case <-deadline:
			t.Fatalf("timeout collecting results, got %d", len(got))
		}
	}
	assert.Equal(t, int64(n), p.Stats().Snapshot().Completed)
	assert.Equal(t, int64(0), p.Pending())
}

func TestPool_SubmitQueueFull(t *testing.T) {
	ctx := context.Background()
	// 1 worker that blocks until released
	release := make(chan struct{})
	p, err := NewPool[int, int](ctx, "t", 1, 1, testLogger(), func(_ context.Context, x int) (int, error) {
		<-release
		return x, nil
	})
	require.NoError(t, err)
	defer func() {
		close(release)
		p.StopNow()
	}()

	// Fill worker + queue
	require.NoError(t, p.SubmitWait(ctx, 1))
	// Give worker time to pick up first item
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, p.Submit(ctx, 2)) // fills queue

	err = p.Submit(ctx, 3)
	assert.ErrorIs(t, err, ErrQueueFull)
}

func TestPool_SubmitWaitCancel(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	p, err := NewPool[int, int](ctx, "t", 1, 1, testLogger(), func(_ context.Context, x int) (int, error) {
		<-release
		return x, nil
	})
	require.NoError(t, err)
	defer func() {
		close(release)
		p.StopNow()
	}()

	require.NoError(t, p.SubmitWait(ctx, 1))
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, p.SubmitWait(ctx, 2)) // fills queue

	cctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	err = p.SubmitWait(cctx, 3)
	assert.Error(t, err)
}

func TestPool_FailedCounted(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool[int, int](ctx, "t", 1, 8, testLogger(), func(_ context.Context, x int) (int, error) {
		if x%2 == 0 {
			return 0, errors.New("even")
		}
		return x, nil
	})
	require.NoError(t, err)

	go Drain(ctx, p.Outputs())

	for i := 0; i < 10; i++ {
		require.NoError(t, p.SubmitWait(ctx, i))
	}
	// Wait until pending zero
	deadline := time.Now().Add(2 * time.Second)
	for p.Pending() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	snap := p.Stats().Snapshot()
	assert.Equal(t, int64(5), snap.Completed)
	assert.Equal(t, int64(5), snap.Failed)
	p.Stop()
}

func TestBridge_Backpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slow := make(chan struct{})
	src, err := NewPool[int, int](ctx, "src", 2, 8, testLogger(), func(_ context.Context, x int) (int, error) {
		return x, nil
	})
	require.NoError(t, err)

	dst, err := NewPool[int, int](ctx, "dst", 1, 2, testLogger(), func(_ context.Context, x int) (int, error) {
		<-slow
		return x, nil
	})
	require.NoError(t, err)

	var bridgeErrs atomic.Int64
	go Bridge(ctx, src.Outputs(), dst, func(error) { bridgeErrs.Add(1) })
	go Drain(ctx, dst.Outputs())

	// Flood source; bridge will block when dst queue is full.
	for i := 0; i < 20; i++ {
		require.NoError(t, src.SubmitWait(ctx, i))
	}

	// Release slow workers
	close(slow)

	deadline := time.Now().Add(3 * time.Second)
	for (src.Pending() > 0 || dst.Pending() > 0) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, int64(0), src.Pending())
	assert.Equal(t, int64(0), dst.Pending())
	assert.Equal(t, int64(20), dst.Stats().Snapshot().Completed)

	src.Stop()
	dst.Stop()
}

func TestBridge_SkipsErrors(t *testing.T) {
	ctx := context.Background()
	src, err := NewPool[int, int](ctx, "src", 1, 8, testLogger(), func(_ context.Context, x int) (int, error) {
		if x == 1 {
			return 0, errors.New("fail")
		}
		return x, nil
	})
	require.NoError(t, err)
	dst, err := NewPool[int, int](ctx, "dst", 1, 8, testLogger(), func(_ context.Context, x int) (int, error) {
		return x, nil
	})
	require.NoError(t, err)

	go Bridge(ctx, src.Outputs(), dst, nil)
	go Drain(ctx, dst.Outputs())

	require.NoError(t, src.SubmitWait(ctx, 1))
	require.NoError(t, src.SubmitWait(ctx, 2))

	deadline := time.Now().Add(2 * time.Second)
	for dst.Stats().Snapshot().Completed < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(t, int64(1), dst.Stats().Snapshot().Completed)
	assert.Equal(t, int64(1), src.Stats().Snapshot().Failed)

	src.Stop()
	dst.Stop()
}

func TestPool_StopIdempotent(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool[int, int](ctx, "t", 1, 4, testLogger(), func(_ context.Context, x int) (int, error) {
		return x, nil
	})
	require.NoError(t, err)
	go Drain(ctx, p.Outputs())
	p.Stop()
	p.Stop()
	p.StopNow()
	assert.True(t, p.IsStopped())
}

func TestPool_ConcurrentSubmit(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool[int, int](ctx, "t", 4, 64, testLogger(), func(_ context.Context, x int) (int, error) {
		return x, nil
	})
	require.NoError(t, err)
	go Drain(ctx, p.Outputs())

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = p.SubmitWait(ctx, i)
		}(i)
	}
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for p.Pending() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(t, int64(n), p.Stats().Snapshot().Completed)
	p.Stop()
}

func TestPool_DiscardOutput_StatsAccurate(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool[int, int](ctx, "terminal", 2, 16, testLogger(), func(_ context.Context, x int) (int, error) {
		if x < 0 {
			return 0, errors.New("neg")
		}
		return x, nil
	}, WithDiscardOutput())
	require.NoError(t, err)
	defer p.Stop()

	assert.True(t, p.DiscardsOutput())
	assert.Nil(t, p.Outputs())

	const n = 20
	for i := 0; i < n; i++ {
		require.NoError(t, p.SubmitWait(ctx, i))
	}
	require.NoError(t, p.SubmitWait(ctx, -1))

	deadline := time.Now().Add(2 * time.Second)
	for p.Pending() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, int64(0), p.Pending())
	snap := p.Stats().Snapshot()
	// Stats come from processItem — discard must not skip counters.
	assert.Equal(t, int64(n), snap.Completed)
	assert.Equal(t, int64(1), snap.Failed)
	assert.Equal(t, int64(n+1), snap.Submitted)
}

func TestDrain_ConsumesUntilClosed(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool[int, int](ctx, "t", 2, 8, testLogger(), func(_ context.Context, x int) (int, error) {
		return x, nil
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		Drain(ctx, p.Outputs())
		close(done)
	}()

	for i := 0; i < 10; i++ {
		require.NoError(t, p.SubmitWait(ctx, i))
	}
	p.Stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not return after pool Stop closed Outputs")
	}
	assert.Equal(t, int64(10), p.Stats().Snapshot().Completed)
}

func TestPool_SubmitWait_BackpressureUnblocks(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	p, err := NewPool[int, int](ctx, "t", 1, 1, testLogger(), func(_ context.Context, x int) (int, error) {
		<-release
		return x, nil
	}, WithDiscardOutput())
	require.NoError(t, err)
	defer p.StopNow()

	require.NoError(t, p.SubmitWait(ctx, 1))
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, p.SubmitWait(ctx, 2))

	blocked := make(chan error, 1)
	go func() {
		blocked <- p.SubmitWait(ctx, 3)
	}()

	select {
	case <-blocked:
		t.Fatal("SubmitWait should block while queue is full")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-blocked:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitWait did not unblock after workers drained")
	}
}
