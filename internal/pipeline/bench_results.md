# Pipeline concurrency bench results

Generated: 2026-07-16T04:49:43-07:00  
Go: go1.24.5  
GOMAXPROCS: 12  

## How to reproduce

```bash
go test ./internal/pipeline/ -run TestConcurrencyBenchReport -count=1 -v
# writes internal/pipeline/bench_results.md
```

Harness compares **old_async_router** (unbounded per-item `go SubmitWait`, idle = pool pending only, then `StopNow`) vs **new_sync_router** (sync saves + bounded requeue workers for feedback, idle includes `routerInflight`).

### Metric definitions

| Metric | Meaning |
|--------|--------|
| **Wall** | End-to-end duration until harness stops |
| **Saves/s** | `Saves ok / Wall` (throughput) |
| **Saves ok / Expected / Lost** | Completeness vs expected job count |
| **G/job** | `Max G / Saves ok` — concurrency cost per completed save |
| **Max G** | Peak `runtime.NumGoroutine()` during the run |
| **Peak HeapInuse (KiB)** | Peak `MemStats.HeapInuse` while running |
| **Peak HeapAlloc (KiB)** | Peak `MemStats.HeapAlloc` while running |
| **Δ HeapInuse (KiB)** | `end.HeapInuse - start.HeapInuse` (after start GC; end not forced GC) |
| **Δ TotalAlloc (KiB)** | Bytes allocated during the run (`TotalAlloc` delta) |
| **GCs** | Number of GC cycles during the run |

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 24214 | 400 | 400 | 0 | 0.23 | 92 | 1464 | 523 | +352 | 147 | 0 |
| new_sync_router | balanced | 146ms | 2736 | 400 | 400 | 0 | 0.06 | 22 | 1200 | 429 | +24 | 14 | 0 |
| old_async_router | slow_saver | 1.044s | 958 | 1000 | 1000 | 0 | 1.00 | 1002 | 2296 | 1267 | +1152 | 845 | 0 |
| new_sync_router | slow_saver | 1.256s | 796 | 1000 | 1000 | 0 | 0.03 | 28 | 1856 | 879 | +48 | 38 | 0 |
| old_async_router | high_feedback | 72ms | 3333 | 240 | 240 | 0 | 1.07 | 256 | 1872 | 945 | +64 | 97 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1816 | 855 | +40 | 10 | 0 |

## Interpretation

- **Primary resource win: Max G and G/job.** Under `slow_saver`, old_async often approaches ~1 goroutine per save; new_sync stays O(workers).
- **Throughput (Saves/s)** is often similar when the saver is the bottleneck — that is expected.
- **Heap columns** measure Go heap; goroutine stacks may not fully dominate `HeapInuse`. Prefer Max G / G/job as the concurrency-cost signal; use Δ TotalAlloc for allocation volume.
- Numbers vary slightly run-to-run (scheduler, GC). Compare old vs new within the same generated file.
## Run: 2026-07-16T06:22:08-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 23774 | 400 | 400 | 0 | 0.23 | 90 | 1440 | 518 | +320 | 136 | 0 |
| new_sync_router | balanced | 161ms | 2484 | 400 | 400 | 0 | 0.06 | 22 | 1216 | 433 | +24 | 15 | 0 |
| old_async_router | slow_saver | 1.044s | 958 | 1000 | 1000 | 0 | 1.00 | 1000 | 2320 | 1262 | +1160 | 842 | 0 |
| new_sync_router | slow_saver | 1.258s | 795 | 1000 | 1000 | 0 | 0.03 | 28 | 1944 | 878 | +72 | 34 | 0 |
| old_async_router | high_feedback | 72ms | 3332 | 240 | 240 | 0 | 1.07 | 256 | 2032 | 956 | +176 | 108 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1888 | 871 | +24 | 10 | 0 |

## Run: 2026-07-16T06:22:12-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 23670 | 400 | 400 | 0 | 0.23 | 91 | 1328 | 518 | +264 | 135 | 0 |
| new_sync_router | balanced | 161ms | 2482 | 400 | 400 | 0 | 0.06 | 22 | 1128 | 429 | +32 | 13 | 0 |
| old_async_router | slow_saver | 1.039s | 963 | 1000 | 1000 | 0 | 1.00 | 1002 | 2352 | 1262 | +1264 | 853 | 0 |
| new_sync_router | slow_saver | 1.256s | 796 | 1000 | 1000 | 0 | 0.03 | 28 | 1824 | 872 | +64 | 40 | 0 |
| old_async_router | high_feedback | 72ms | 3335 | 240 | 240 | 0 | 1.07 | 256 | 1832 | 947 | +80 | 105 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1816 | 856 | +56 | 10 | 0 |

## Run: 2026-07-21T08:01:26-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 23806 | 400 | 400 | 0 | 0.23 | 91 | 1328 | 508 | +336 | 135 | 0 |
| new_sync_router | balanced | 161ms | 2480 | 400 | 400 | 0 | 0.06 | 22 | 1112 | 422 | +32 | 15 | 0 |
| old_async_router | slow_saver | 1.044s | 958 | 1000 | 1000 | 0 | 1.00 | 1000 | 2312 | 1268 | +1256 | 861 | 0 |
| new_sync_router | slow_saver | 1.256s | 796 | 1000 | 1000 | 0 | 0.03 | 28 | 1856 | 881 | +48 | 31 | 0 |
| old_async_router | high_feedback | 72ms | 3336 | 240 | 240 | 0 | 1.07 | 256 | 1848 | 953 | +64 | 101 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1808 | 862 | +48 | 10 | 0 |

## Run: 2026-07-21T08:17:47-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 23874 | 400 | 400 | 0 | 0.23 | 90 | 1376 | 523 | +312 | 141 | 0 |
| new_sync_router | balanced | 146ms | 2735 | 400 | 400 | 0 | 0.06 | 22 | 1168 | 438 | +32 | 13 | 0 |
| old_async_router | slow_saver | 1.044s | 958 | 1000 | 1000 | 0 | 1.00 | 1002 | 2320 | 1261 | +1224 | 843 | 0 |
| new_sync_router | slow_saver | 1.256s | 796 | 1000 | 1000 | 0 | 0.03 | 28 | 1888 | 870 | +72 | 32 | 0 |
| old_async_router | high_feedback | 72ms | 3335 | 240 | 240 | 0 | 1.07 | 256 | 1896 | 950 | +88 | 110 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1848 | 861 | +48 | 12 | 0 |

## Run: 2026-07-21T08:24:24-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 24010 | 400 | 400 | 0 | 0.23 | 90 | 1352 | 523 | +248 | 145 | 0 |
| new_sync_router | balanced | 146ms | 2736 | 400 | 400 | 0 | 0.06 | 22 | 1192 | 427 | +16 | 11 | 0 |
| old_async_router | slow_saver | 1.043s | 959 | 1000 | 1000 | 0 | 1.00 | 1002 | 2288 | 1305 | +1136 | 889 | 0 |
| new_sync_router | slow_saver | 1.254s | 798 | 1000 | 1000 | 0 | 0.03 | 28 | 1960 | 909 | +48 | 31 | 0 |
| old_async_router | high_feedback | 72ms | 3335 | 240 | 240 | 0 | 1.07 | 256 | 1952 | 979 | +64 | 100 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1928 | 890 | +40 | 10 | 0 |

## Run: 2026-07-21T10:19:46-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 23566 | 400 | 400 | 0 | 0.23 | 91 | 1336 | 517 | +320 | 139 | 0 |
| new_sync_router | balanced | 146ms | 2734 | 400 | 400 | 0 | 0.06 | 22 | 1152 | 435 | +24 | 15 | 0 |
| old_async_router | slow_saver | 1.047s | 955 | 1000 | 1000 | 0 | 1.00 | 1004 | 2280 | 1294 | +1176 | 879 | 0 |
| new_sync_router | slow_saver | 1.255s | 797 | 1000 | 1000 | 0 | 0.03 | 28 | 1920 | 892 | +56 | 35 | 0 |
| old_async_router | high_feedback | 72ms | 3334 | 240 | 240 | 0 | 1.07 | 256 | 1944 | 966 | +80 | 104 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1904 | 881 | +56 | 10 | 0 |

## Run: 2026-07-21T10:20:40-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 24206 | 400 | 400 | 0 | 0.23 | 90 | 1408 | 530 | +336 | 157 | 0 |
| new_sync_router | balanced | 146ms | 2734 | 400 | 400 | 0 | 0.06 | 22 | 1200 | 441 | +24 | 13 | 0 |
| old_async_router | slow_saver | 1.042s | 960 | 1000 | 1000 | 0 | 1.00 | 1004 | 2296 | 1295 | +1136 | 874 | 0 |
| new_sync_router | slow_saver | 1.256s | 796 | 1000 | 1000 | 0 | 0.03 | 28 | 1912 | 898 | +32 | 31 | 0 |
| old_async_router | high_feedback | 72ms | 3335 | 240 | 240 | 0 | 1.07 | 256 | 1984 | 969 | +120 | 101 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1928 | 889 | +40 | 11 | 0 |

## Run: 2026-07-21T13:01:35-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 24222 | 400 | 400 | 0 | 0.23 | 94 | 1440 | 518 | +296 | 139 | 0 |
| new_sync_router | balanced | 161ms | 2483 | 400 | 400 | 0 | 0.06 | 22 | 1248 | 433 | +24 | 15 | 0 |
| old_async_router | slow_saver | 1.042s | 959 | 1000 | 1000 | 0 | 1.00 | 1002 | 2232 | 1287 | +1048 | 868 | 0 |
| new_sync_router | slow_saver | 1.257s | 795 | 1000 | 1000 | 0 | 0.03 | 28 | 1968 | 896 | +56 | 36 | 0 |
| old_async_router | high_feedback | 72ms | 3337 | 240 | 240 | 0 | 1.07 | 256 | 2000 | 973 | +96 | 108 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1928 | 888 | +40 | 10 | 0 |

## Run: 2026-07-21T13:06:35-07:00

Go: go1.24.5 · GOMAXPROCS: 12

| Model | Scenario | Wall | Saves/s | Saves ok | Expected | Lost | G/job | Max G | Peak Inuse KiB | Peak Alloc KiB | Δ Inuse KiB | Δ TotalAlloc KiB | GCs |
|-------|----------|------|---------|----------|----------|------|-------|-------|----------------|----------------|-------------|------------------|-----|
| old_async_router | balanced | 17ms | 23986 | 400 | 400 | 0 | 0.23 | 91 | 1352 | 518 | +320 | 139 | 0 |
| new_sync_router | balanced | 146ms | 2736 | 400 | 400 | 0 | 0.06 | 22 | 1152 | 432 | +24 | 16 | 0 |
| old_async_router | slow_saver | 1.045s | 957 | 1000 | 1000 | 0 | 1.00 | 1000 | 2328 | 1263 | +1224 | 845 | 0 |
| new_sync_router | slow_saver | 1.256s | 796 | 1000 | 1000 | 0 | 0.03 | 28 | 1872 | 877 | +48 | 32 | 0 |
| old_async_router | high_feedback | 72ms | 3336 | 240 | 240 | 0 | 1.07 | 256 | 1872 | 949 | +88 | 108 | 0 |
| new_sync_router | high_feedback | 287ms | 836 | 240 | 240 | 0 | 0.08 | 20 | 1832 | 866 | +48 | 10 | 0 |

