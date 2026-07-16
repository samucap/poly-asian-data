package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// Mock pipeline concurrency harness: old async-router vs new bounded-requeue router.
// No HTTP / DB.
//
// Append a run section to internal/pipeline/bench_results.md (header written once):
//
//	go test ./internal/pipeline/ -run TestConcurrencyBenchReport -count=1 -v

type benchJob struct {
	ID    int
	Depth int
}

type benchProcOut struct {
	Saves    []int
	Feedback []benchJob
}

type benchConfig struct {
	Name           string
	N              int
	FetchWorkers   int
	ProcWorkers    int
	SaveWorkers    int
	Queue          int
	FetchLatency   time.Duration
	ProcLatency    time.Duration
	SaveLatency    time.Duration
	DerivedPerItem int
	FeedbackDepth  int
	StableIdle     time.Duration
}

type memSnap struct {
	HeapAlloc  uint64
	HeapInuse  uint64
	TotalAlloc uint64
	NumGC      uint32
}

type runtimeSample struct {
	maxG           int
	peakHeapAlloc  uint64
	peakHeapInuse  uint64
	start          memSnap
	end            memSnap
}

type benchResult struct {
	Model       string
	Scenario    string
	Wall        time.Duration
	Seeded      int64
	FetchDone   int64
	ProcDone    int64
	SavesDone   int64
	ExpectedMin int64
	Lost        int64

	// Resource metrics
	MaxG          int
	GPerJob       float64 // MaxG / SavesDone
	SavesPerSec   float64
	PeakHeapAlloc uint64
	PeakHeapInuse uint64
	DeltaHeapInuse int64  // end.HeapInuse - start.HeapInuse (can be negative after GC)
	DeltaTotalAlloc uint64 // end.TotalAlloc - start.TotalAlloc (bytes allocated during run)
	NumGC         uint32
}

func loggerDiscard() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func expectedSaves(cfg benchConfig) int64 {
	total := int64(0)
	cur := int64(cfg.N)
	for d := 0; d <= cfg.FeedbackDepth; d++ {
		total += cur
		if d == cfg.FeedbackDepth {
			break
		}
		cur = cur * int64(cfg.DerivedPerItem)
	}
	return total
}

func readMem() memSnap {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return memSnap{
		HeapAlloc:  ms.HeapAlloc,
		HeapInuse:  ms.HeapInuse,
		TotalAlloc: ms.TotalAlloc,
		NumGC:      ms.NumGC,
	}
}

func readMemNoGC() memSnap {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return memSnap{
		HeapAlloc:  ms.HeapAlloc,
		HeapInuse:  ms.HeapInuse,
		TotalAlloc: ms.TotalAlloc,
		NumGC:      ms.NumGC,
	}
}

func sampleRuntime(stop <-chan struct{}, samp *runtimeSample, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.NewTicker(2 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if g := runtime.NumGoroutine(); g > samp.maxG {
				samp.maxG = g
			}
			ms := readMemNoGC()
			if ms.HeapAlloc > samp.peakHeapAlloc {
				samp.peakHeapAlloc = ms.HeapAlloc
			}
			if ms.HeapInuse > samp.peakHeapInuse {
				samp.peakHeapInuse = ms.HeapInuse
			}
		}
	}
}

// runOldAsync: unbounded go SubmitWait per item; idle = pool pending only; then StopNow.
func runOldAsync(cfg benchConfig) benchResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := loggerDiscard()

	var savesDone atomic.Int64
	fetch, proc, save := newMockStages(ctx, log, cfg, &savesDone)

	workerpool.StartBridge(ctx, fetch.Outputs(), proc, nil)
	workerpool.StartDrain(ctx, save.Outputs())

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-proc.Outputs():
				if !ok {
					return
				}
				if res.Err != nil {
					continue
				}
				out := res.Value
				for _, id := range out.Saves {
					id := id
					// Simulate scheduler delay before Submit — race window for idle.
					go func() {
						time.Sleep(2 * time.Millisecond)
						_ = save.SubmitWait(ctx, id)
					}()
				}
				for _, fb := range out.Feedback {
					fb := fb
					go func() {
						time.Sleep(2 * time.Millisecond)
						_ = fetch.SubmitWait(ctx, fb)
					}()
				}
			}
		}
	}()

	var samp runtimeSample
	samp.start = readMem()
	stopSamp := make(chan struct{})
	var sampWG sync.WaitGroup
	sampWG.Add(1)
	go sampleRuntime(stopSamp, &samp, &sampWG)

	start := time.Now()
	for i := 0; i < cfg.N; i++ {
		_ = fetch.SubmitWait(ctx, benchJob{ID: i, Depth: 0})
	}

	waitPoolsFirstIdle(ctx, fetch, proc, save, int64(cfg.N))

	cancel()
	fetch.StopNow()
	proc.StopNow()
	save.StopNow()
	close(stopSamp)
	sampWG.Wait()
	samp.end = readMemNoGC()

	return makeResult("old_async_router", cfg, start, fetch, proc, savesDone.Load(), &samp)
}

// runNewSync: saves sync; feedback via bounded requeue workers; idle includes routerInflight.
func runNewSync(cfg benchConfig) benchResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := loggerDiscard()

	var savesDone atomic.Int64
	var routerInflight atomic.Int64
	fetch, proc, save := newMockStages(ctx, log, cfg, &savesDone)

	workerpool.StartBridge(ctx, fetch.Outputs(), proc, nil)
	workerpool.StartDrain(ctx, save.Outputs())

	const requeueWorkers = 4
	requeue := make(chan benchJob, 128)
	for i := 0; i < requeueWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case j, ok := <-requeue:
					if !ok {
						return
					}
					_ = fetch.SubmitWait(ctx, j)
					routerInflight.Add(-1)
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-proc.Outputs():
				if !ok {
					return
				}
				if res.Err != nil {
					continue
				}
				routerInflight.Add(1)
				out := res.Value
				for _, id := range out.Saves {
					_ = save.SubmitWait(ctx, id)
				}
				for _, fb := range out.Feedback {
					routerInflight.Add(1)
					select {
					case <-ctx.Done():
						routerInflight.Add(-1)
						routerInflight.Add(-1)
						return
					case requeue <- fb:
					}
				}
				routerInflight.Add(-1)
			}
		}
	}()

	var samp runtimeSample
	samp.start = readMem()
	stopSamp := make(chan struct{})
	var sampWG sync.WaitGroup
	sampWG.Add(1)
	go sampleRuntime(stopSamp, &samp, &sampWG)

	start := time.Now()
	for i := 0; i < cfg.N; i++ {
		_ = fetch.SubmitWait(ctx, benchJob{ID: i, Depth: 0})
	}

	stableFor := cfg.StableIdle
	if stableFor <= 0 {
		stableFor = 150 * time.Millisecond
	}
	waitPoolsIdleWithRouter(ctx, fetch, proc, save, &routerInflight, stableFor)

	fetch.Stop()
	proc.Stop()
	save.Stop()
	cancel()
	close(stopSamp)
	sampWG.Wait()
	samp.end = readMemNoGC()

	return makeResult("new_sync_router", cfg, start, fetch, proc, savesDone.Load(), &samp)
}

func newMockStages(ctx context.Context, log *slog.Logger, cfg benchConfig, savesDone *atomic.Int64) (
	*workerpool.Pool[benchJob, benchJob],
	*workerpool.Pool[benchJob, benchProcOut],
	*workerpool.Pool[int, int],
) {
	fetch, _ := workerpool.NewPool[benchJob, benchJob](ctx, "f", cfg.FetchWorkers, cfg.Queue, log,
		func(_ context.Context, j benchJob) (benchJob, error) {
			if cfg.FetchLatency > 0 {
				time.Sleep(cfg.FetchLatency)
			}
			return j, nil
		})
	proc, _ := workerpool.NewPool[benchJob, benchProcOut](ctx, "p", cfg.ProcWorkers, cfg.Queue, log,
		func(_ context.Context, j benchJob) (benchProcOut, error) {
			if cfg.ProcLatency > 0 {
				time.Sleep(cfg.ProcLatency)
			}
			out := benchProcOut{Saves: []int{j.ID}}
			if j.Depth < cfg.FeedbackDepth {
				for i := 0; i < cfg.DerivedPerItem; i++ {
					out.Feedback = append(out.Feedback, benchJob{
						ID:    j.ID*1000 + i + j.Depth,
						Depth: j.Depth + 1,
					})
				}
			}
			return out, nil
		})
	save, _ := workerpool.NewPool[int, int](ctx, "s", cfg.SaveWorkers, cfg.Queue, log,
		func(_ context.Context, id int) (int, error) {
			if cfg.SaveLatency > 0 {
				time.Sleep(cfg.SaveLatency)
			}
			savesDone.Add(1)
			return id, nil
		})
	return fetch, proc, save
}

func makeResult(model string, cfg benchConfig, start time.Time, fetch, proc interface {
	Stats() *workerpool.Stats
}, saves int64, samp *runtimeSample) benchResult {
	exp := expectedSaves(cfg)
	lost := exp - saves
	if lost < 0 {
		lost = 0
	}
	wall := time.Since(start)
	var gPerJob float64
	var sps float64
	if saves > 0 {
		gPerJob = float64(samp.maxG) / float64(saves)
		sps = float64(saves) / wall.Seconds()
	}
	var deltaInuse int64
	if samp.end.HeapInuse >= samp.start.HeapInuse {
		deltaInuse = int64(samp.end.HeapInuse - samp.start.HeapInuse)
	} else {
		deltaInuse = -int64(samp.start.HeapInuse - samp.end.HeapInuse)
	}
	var deltaTotal uint64
	if samp.end.TotalAlloc >= samp.start.TotalAlloc {
		deltaTotal = samp.end.TotalAlloc - samp.start.TotalAlloc
	}
	return benchResult{
		Model:            model,
		Scenario:         cfg.Name,
		Wall:             wall,
		Seeded:           int64(cfg.N),
		FetchDone:        fetch.Stats().Snapshot().Completed,
		ProcDone:         proc.Stats().Snapshot().Completed,
		SavesDone:        saves,
		ExpectedMin:      exp,
		Lost:             lost,
		MaxG:             samp.maxG,
		GPerJob:          gPerJob,
		SavesPerSec:      sps,
		PeakHeapAlloc:    samp.peakHeapAlloc,
		PeakHeapInuse:    samp.peakHeapInuse,
		DeltaHeapInuse:   deltaInuse,
		DeltaTotalAlloc:  deltaTotal,
		NumGC:            samp.end.NumGC - samp.start.NumGC,
	}
}

func waitPoolsFirstIdle(ctx context.Context, a, b, c interface{ Pending() int64 }, minSubmitted int64) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(20 * time.Second)
	_ = minSubmitted
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-ticker.C:
			pending := a.Pending() + b.Pending() + c.Pending()
			if pending == 0 {
				time.Sleep(1 * time.Millisecond)
				if a.Pending()+b.Pending()+c.Pending() == 0 {
					return
				}
			}
		}
	}
}

func waitPoolsIdleWithRouter(ctx context.Context, a, b, c interface{ Pending() int64 }, inflight *atomic.Int64, stable time.Duration) {
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	var since time.Time
	ok := false
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-ticker.C:
			idle := a.Pending() == 0 && b.Pending() == 0 && c.Pending() == 0 && inflight.Load() == 0
			if idle {
				if !ok {
					since = time.Now()
					ok = true
				} else if time.Since(since) >= stable {
					return
				}
			} else {
				ok = false
			}
		}
	}
}

func kib(b uint64) uint64 { return b / 1024 }
func kibSigned(b int64) int64 {
	return b / 1024
}

func formatResultRow(r benchResult) string {
	return fmt.Sprintf("| %s | %s | %s | %.0f | %d | %d | %d | %.2f | %d | %d | %d | %+d | %d | %d |",
		r.Model,
		r.Scenario,
		r.Wall.Round(time.Millisecond),
		r.SavesPerSec,
		r.SavesDone,
		r.ExpectedMin,
		r.Lost,
		r.GPerJob,
		r.MaxG,
		kib(r.PeakHeapInuse),
		kib(r.PeakHeapAlloc),
		kibSigned(r.DeltaHeapInuse),
		kib(r.DeltaTotalAlloc),
		r.NumGC,
	)
}

func TestConcurrencyBenchReport(t *testing.T) {
	scenarios := []benchConfig{
		{
			Name: "balanced",
			N: 400, FetchWorkers: 4, ProcWorkers: 4, SaveWorkers: 4, Queue: 32,
			FetchLatency: 100 * time.Microsecond, ProcLatency: 100 * time.Microsecond, SaveLatency: 100 * time.Microsecond,
			StableIdle: 120 * time.Millisecond,
		},
		{
			Name: "slow_saver",
			N: 1000, FetchWorkers: 8, ProcWorkers: 8, SaveWorkers: 2, Queue: 16,
			FetchLatency: 20 * time.Microsecond, ProcLatency: 20 * time.Microsecond, SaveLatency: 2 * time.Millisecond,
			StableIdle: 200 * time.Millisecond,
		},
		{
			Name: "high_feedback",
			N: 80, FetchWorkers: 4, ProcWorkers: 4, SaveWorkers: 2, Queue: 32,
			FetchLatency: 50 * time.Microsecond, ProcLatency: 50 * time.Microsecond, SaveLatency: 500 * time.Microsecond,
			DerivedPerItem: 2, FeedbackDepth: 1,
			StableIdle: 200 * time.Millisecond,
		},
	}

	var ordered []benchResult
	for _, sc := range scenarios {
		t.Logf("running scenario %s old...", sc.Name)
		ordered = append(ordered, runOldAsync(sc))
		t.Logf("running scenario %s new...", sc.Name)
		nr := runNewSync(sc)
		ordered = append(ordered, nr)
		if nr.Lost > 0 {
			t.Errorf("new_sync_router lost work in %s: expected>=%d got=%d lost=%d",
				sc.Name, nr.ExpectedMin, nr.SavesDone, nr.Lost)
		}
		workers := sc.FetchWorkers + sc.ProcWorkers + sc.SaveWorkers
		if nr.MaxG > workers+80 {
			t.Logf("warning: new maxG=%d workers=%d scenario=%s", nr.MaxG, workers, sc.Name)
		}
	}

	// Append next to this test package so path is stable regardless of -cwd.
	// First run writes header + interpretation; later runs only append a ## Run section.
	path := filepath.Join("bench_results.md")
	if err := appendBenchResults(path, ordered); err != nil {
		t.Fatalf("write report: %v", err)
	}

	for _, r := range ordered {
		t.Logf("%s %s wall=%s saves/s=%.0f maxG=%d g/job=%.2f peakInuseKiB=%d ΔtotalKiB=%d",
			r.Model, r.Scenario, r.Wall, r.SavesPerSec, r.MaxG, r.GPerJob,
			kib(r.PeakHeapInuse), kib(r.DeltaTotalAlloc))
	}
}

// appendBenchResults writes or appends to bench_results.md.
// Empty/new file → full header + metric definitions + interpretation once.
// Existing file → append "## Run: ..." table only.
func appendBenchResults(path string, ordered []benchResult) error {
	info, err := os.Stat(path)
	needHeader := err != nil && os.IsNotExist(err)
	if err == nil && info.Size() == 0 {
		needHeader = true
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if needHeader {
		fmt.Fprintf(f, "# Pipeline concurrency bench results\n\n")
		fmt.Fprintf(f, "## How to reproduce\n\n")
		fmt.Fprintf(f, "```bash\n")
		fmt.Fprintf(f, "go test ./internal/pipeline/ -run TestConcurrencyBenchReport -count=1 -v\n")
		fmt.Fprintf(f, "# appends a ## Run section to internal/pipeline/bench_results.md\n")
		fmt.Fprintf(f, "```\n\n")
		fmt.Fprintf(f, "Harness compares **old_async_router** (unbounded per-item `go SubmitWait`, idle = pool pending only, then `StopNow`) vs **new_sync_router** (sync saves + bounded requeue workers for feedback, idle includes `routerInflight`).\n\n")
		fmt.Fprintf(f, "### Metric definitions\n\n")
		fmt.Fprintf(f, "| Metric | Meaning |\n")
		fmt.Fprintf(f, "|--------|--------|\n")
		fmt.Fprintf(f, "| **Wall** | End-to-end duration until harness stops |\n")
		fmt.Fprintf(f, "| **Saves/s** | `Saves ok / Wall` (throughput) |\n")
		fmt.Fprintf(f, "| **Saves ok / Expected / Lost** | Completeness vs expected job count |\n")
		fmt.Fprintf(f, "| **G/job** | `Max G / Saves ok` — concurrency cost per completed save |\n")
		fmt.Fprintf(f, "| **Max G** | Peak `runtime.NumGoroutine()` during the run |\n")
		fmt.Fprintf(f, "| **Peak HeapInuse (KiB)** | Peak `MemStats.HeapInuse` while running |\n")
		fmt.Fprintf(f, "| **Peak HeapAlloc (KiB)** | Peak `MemStats.HeapAlloc` while running |\n")
		fmt.Fprintf(f, "| **Δ HeapInuse (KiB)** | `end.HeapInuse - start.HeapInuse` (after start GC; end not forced GC) |\n")
		fmt.Fprintf(f, "| **Δ TotalAlloc (KiB)** | Bytes allocated during the run (`TotalAlloc` delta) |\n")
		fmt.Fprintf(f, "| **GCs** | Number of GC cycles during the run |\n\n")
		fmt.Fprintf(f, "## Interpretation\n\n")
		fmt.Fprintf(f, "- **Primary resource win: Max G and G/job.** Under `slow_saver`, old_async often approaches ~1 goroutine per save; new_sync stays O(workers).\n")
		fmt.Fprintf(f, "- **Throughput (Saves/s)** is often similar when the saver is the bottleneck — that is expected.\n")
		fmt.Fprintf(f, "- **Heap columns** measure Go heap; goroutine stacks may not fully dominate `HeapInuse`. Prefer Max G / G/job as the concurrency-cost signal; use Δ TotalAlloc for allocation volume.\n")
		fmt.Fprintf(f, "- Numbers vary slightly run-to-run (scheduler, GC). Compare old vs new within the same run section; history is preserved by appending runs.\n\n")
	}

	fmt.Fprintf(f, "## Run: %s\n\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(f, "Go: %s · GOMAXPROCS: %d\n\n", runtime.Version(), runtime.GOMAXPROCS(0))
	fmt.Fprintf(f, "| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |\n")
	fmt.Fprintf(f, "|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|\n")
	for _, r := range ordered {
		fmt.Fprintln(f, formatResultRow(r))
	}
	fmt.Fprintln(f)
	return nil
}

func BenchmarkPipelineConcurrency(b *testing.B) {
	cfg := benchConfig{
		Name: "bench_slow_saver",
		N: 150, FetchWorkers: 4, ProcWorkers: 4, SaveWorkers: 2, Queue: 16,
		SaveLatency: 1 * time.Millisecond,
		StableIdle:  80 * time.Millisecond,
	}
	b.Run("old_async", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = runOldAsync(cfg)
		}
	})
	b.Run("new_sync", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = runNewSync(cfg)
		}
	})
}
