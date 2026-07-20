package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// routeProcessorOutput fans out processor results:
//   - Saver payloads → saver.SubmitWait (backpressure)
//   - Derived / next-page requests → bounded requeue into fetcher
//
// Requeue never blocks the processor drain path: full channel tries non-blocking
// fetcher submit, then a capped overflow list drained by requeue workers.
func (p *Pipeline) routeProcessorOutput(ctx context.Context) {
	const requeueWorkers = 4
	requeueBuf := 256
	if q := p.cfg.FetcherCfg.Qsize; q > requeueBuf {
		requeueBuf = q * 2
	}
	requeue := make(chan *fetcher.Request, requeueBuf)

	const maxOverflow = 50_000
	var (
		overflowMu sync.Mutex
		overflow   []*fetcher.Request
	)

	submitToFetcher := func(req *fetcher.Request) {
		if req == nil {
			p.routerInflight.Add(-1)
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

	popOverflow := func() *fetcher.Request {
		overflowMu.Lock()
		defer overflowMu.Unlock()
		if len(overflow) == 0 {
			return nil
		}
		req := overflow[0]
		overflow[0] = nil
		overflow = overflow[1:]
		return req
	}

	for i := 0; i < requeueWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case req, ok := <-requeue:
					if !ok {
						return
					}
					submitToFetcher(req)
					continue
				default:
				}

				if req := popOverflow(); req != nil {
					submitToFetcher(req)
					continue
				}

				select {
				case <-ctx.Done():
					return
				case req, ok := <-requeue:
					if !ok {
						return
					}
					submitToFetcher(req)
				}
			}
		}()
	}

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
			return
		default:
			if err := p.fetcherPool.Submit(ctx, req); err == nil {
				p.routerInflight.Add(-1)
				return
			} else if err != workerpool.ErrQueueFull && ctx.Err() != nil {
				p.routerInflight.Add(-1)
				return
			}
			overflowMu.Lock()
			if len(overflow) >= maxOverflow {
				overflowMu.Unlock()
				p.logger.Warn("dropping derived request: requeue overflow full",
					slog.String("url", req.URL),
					slog.Int("max_overflow", maxOverflow),
				)
				p.routerInflight.Add(-1)
				return
			}
			overflow = append(overflow, req)
			overflowMu.Unlock()
		}
	}

	for {
		select {
		case <-ctx.Done():
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
