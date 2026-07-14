package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/saver"
)

// routeProcessorOutput is the feedback/router loop.
//
// Saves are submitted synchronously (backpressure into saver).
// Derived/next-page requests go through a bounded requeue channel with a small
// set of worker goroutines. That keeps feedback concurrency bounded while
// allowing the router to keep draining processor output (avoids cyclic deadlock
// when feedback multiplies work under full queues).
func (p *Pipeline) routeProcessorOutput(ctx context.Context) {
	const requeueWorkers = 4
	// Buffer absorbs bursts of derived requests (e.g. orderbook chunks).
	requeueBuf := 256
	if q := p.cfg.FetcherCfg.Qsize; q > requeueBuf {
		requeueBuf = q * 2
	}
	requeue := make(chan *fetcher.Request, requeueBuf)

	var requeueWG sync.WaitGroup
	for i := 0; i < requeueWorkers; i++ {
		requeueWG.Add(1)
		go func() {
			defer requeueWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case req, ok := <-requeue:
					if !ok {
						return
					}
					if err := p.fetcherPool.SubmitWait(ctx, req); err != nil {
						p.logger.Warn("failed to submit requeued request",
							slog.String("url", req.URL),
							slog.String("error", err.Error()),
						)
					}
					p.routerInflight.Add(-1)
				}
			}
		}()
	}

	// Serialize cursor advances per sync type so concurrent pages stay monotonic.
	var cursorMu sync.Mutex
	lastCursor := make(map[string]string)

	enqueueFetch := func(req *fetcher.Request) {
		if req == nil {
			return
		}
		p.routerInflight.Add(1)
		select {
		case <-ctx.Done():
			p.routerInflight.Add(-1)
			return
		case requeue <- req:
		}
	}

	for {
		select {
		case <-ctx.Done():
			// Leave requeue workers to exit via ctx; do not close requeue while they select on it
			// with ctx — they return on Done.
			return
		case result, ok := <-p.processorPool.Outputs():
			if !ok {
				return
			}
			if result.Err != nil {
				continue
			}
			output := result.Value
			if output == nil {
				p.logger.Warn("processor returned nil output with no error")
				continue
			}

			p.routerInflight.Add(1)

			// 1. Saver payloads — sync backpressure into saver
			for _, payload := range output.SaverPayloads {
				record := &saver.Record{
					ID:          output.ID,
					TableName:   payload.TableName,
					Data:        payload.Data,
					ItemCount:   output.ItemCount,
					ProcessedAt: output.ProcessedAt,
				}
				if err := p.saverPool.SubmitWait(ctx, record); err != nil {
					p.logger.Warn("failed to submit to saver",
						slog.String("table", payload.TableName),
						slog.String("error", err.Error()),
					)
					if ctx.Err() != nil {
						p.routerInflight.Add(-1)
						return
					}
				}
			}

			// 2. Cursor update (ordered)
			if output.SyncType != "" && output.LastCursor != "" {
				cursorMu.Lock()
				prev := lastCursor[output.SyncType]
				if output.LastCursor > prev {
					lastCursor[output.SyncType] = output.LastCursor
					if err := p.saverPool.SetSyncCursor(ctx, output.SyncType, output.LastCursor, output.ItemCount); err != nil {
						p.logger.Warn("failed to update sync cursor",
							slog.String("syncType", output.SyncType),
							slog.String("cursor", output.LastCursor),
							slog.Any("error", err),
						)
					}
				}
				cursorMu.Unlock()
			}

			// 3. Derived + next page via bounded requeue
			for _, req := range output.DerivedRequests {
				enqueueFetch(req)
			}
			nextReq := output.NextPageRequest
			if nextReq == nil && output.OriginalRequest != nil {
				nextReq = p.fetcherPool.BuildNextPageRequest(output.OriginalRequest, output.ItemCount)
			}
			if nextReq != nil {
				enqueueFetch(nextReq)
			}

			p.routerInflight.Add(-1)
		}
	}
}
