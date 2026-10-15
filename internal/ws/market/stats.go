package market

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/term"
)

// RuntimeStats holds process-wide counters for the status line (thread-safe).
type RuntimeStats struct {
	start time.Time

	msgTotal   atomic.Uint64
	msgBook    atomic.Uint64
	msgPC      atomic.Uint64
	msgBBA     atomic.Uint64
	msgTrade   atomic.Uint64
	msgTick    atomic.Uint64
	msgNewMkt  atomic.Uint64
	msgResolve atomic.Uint64
	msgOther   atomic.Uint64

	signalsTotal atomic.Uint64
	dbFeatBatch  atomic.Uint64
	dbFeatRows   atomic.Uint64
	dbSnapBatch  atomic.Uint64
	dbSnapRows   atomic.Uint64
	resolveQueued atomic.Uint64
	resolveFlushed atomic.Uint64
	reconnects   atomic.Uint64

	// rate window
	mu       sync.Mutex
	prevMsg  uint64
	prevSig  uint64
	prevAt   time.Time
	lastMsgS float64
	lastSigM float64 // signals per minute
}

// NewRuntimeStats starts counters at now.
func NewRuntimeStats() *RuntimeStats {
	return &RuntimeStats{start: time.Now(), prevAt: time.Now()}
}

// ObserveMsg increments message counters by event type.
func (s *RuntimeStats) ObserveMsg(eventType string) {
	if s == nil {
		return
	}
	s.msgTotal.Add(1)
	switch eventType {
	case EventBook:
		s.msgBook.Add(1)
	case EventPriceChange:
		s.msgPC.Add(1)
	case EventBestBidAsk:
		s.msgBBA.Add(1)
	case EventLastTradePrice:
		s.msgTrade.Add(1)
	case EventTickSizeChange:
		s.msgTick.Add(1)
	case EventNewMarket:
		s.msgNewMkt.Add(1)
	case EventMarketResolved:
		s.msgResolve.Add(1)
	default:
		s.msgOther.Add(1)
	}
}

func (s *RuntimeStats) AddSignals(n int) {
	if s == nil || n <= 0 {
		return
	}
	s.signalsTotal.Add(uint64(n))
}

func (s *RuntimeStats) AddFeatFlush(rows int) {
	if s == nil {
		return
	}
	s.dbFeatBatch.Add(1)
	if rows > 0 {
		s.dbFeatRows.Add(uint64(rows))
	}
}

func (s *RuntimeStats) AddSnapFlush(rows int) {
	if s == nil {
		return
	}
	s.dbSnapBatch.Add(1)
	if rows > 0 {
		s.dbSnapRows.Add(uint64(rows))
	}
}

func (s *RuntimeStats) AddResolveQueued(n int) {
	if s == nil || n <= 0 {
		return
	}
	s.resolveQueued.Add(uint64(n))
}

func (s *RuntimeStats) AddResolveFlushed(n int) {
	if s == nil || n <= 0 {
		return
	}
	s.resolveFlushed.Add(uint64(n))
}

func (s *RuntimeStats) IncReconnect() {
	if s != nil {
		s.reconnects.Add(1)
	}
}

// Snapshot rates since last SnapshotRates call.
func (s *RuntimeStats) snapshotRates() (msgPerSec, sigPerMin float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	dt := now.Sub(s.prevAt).Seconds()
	if dt < 0.001 {
		return s.lastMsgS, s.lastSigM
	}
	msg := s.msgTotal.Load()
	sig := s.signalsTotal.Load()
	s.lastMsgS = float64(msg-s.prevMsg) / dt
	s.lastSigM = float64(sig-s.prevSig) / dt * 60
	s.prevMsg = msg
	s.prevSig = sig
	s.prevAt = now
	return s.lastMsgS, s.lastSigM
}

// StatusInput is live gauges for Render.
type StatusInput struct {
	Sub     int
	Mem     int
	Pending int
}

// Line builds one status line (no trailing newline).
func (s *RuntimeStats) Line(in StatusInput) string {
	if s == nil {
		return ""
	}
	msgS, sigM := s.snapshotRates()
	up := time.Since(s.start).Truncate(time.Second)
	return fmt.Sprintf(
		"ws up=%s sub=%d mem=%d  msg/s=%.0f [book=%d pc=%d bba=%d tr=%d res=%d tick=%d new=%d]  signals=%d (+%.1f/m)  db_feat=%d db_snap=%d pending=%d  res_q=%d res_db=%d",
		up,
		in.Sub, in.Mem,
		msgS,
		s.msgBook.Load(), s.msgPC.Load(), s.msgBBA.Load(), s.msgTrade.Load(),
		s.msgResolve.Load(), s.msgTick.Load(), s.msgNewMkt.Load(),
		s.signalsTotal.Load(), sigM,
		s.dbFeatBatch.Load(), s.dbSnapBatch.Load(), in.Pending,
		s.resolveQueued.Load(), s.resolveFlushed.Load(),
	)
}

// IsTTY reports whether fd is a terminal (for in-place status).
func IsTTY(f *os.File) bool {
	if f == nil {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

// WriteStatusLine writes in-place on TTY (stderr preferred so logs on stdout stay clean).
func WriteStatusLine(line string) {
	// Clear to end of line then return to start
	_, _ = fmt.Fprintf(os.Stderr, "\r\033[K%s", line)
}

// FinishStatusLine ends the in-place line with a newline (on shutdown).
func FinishStatusLine() {
	_, _ = fmt.Fprint(os.Stderr, "\n")
}

// Attrs for structured periodic logging (non-TTY).
func (s *RuntimeStats) Attrs(in StatusInput) []any {
	msgS, sigM := s.snapshotRates()
	return []any{
		"up", time.Since(s.start).Truncate(time.Second).String(),
		"sub", in.Sub,
		"mem", in.Mem,
		"msg_per_s", fmt.Sprintf("%.1f", msgS),
		"msg_book", s.msgBook.Load(),
		"msg_pc", s.msgPC.Load(),
		"msg_bba", s.msgBBA.Load(),
		"msg_tr", s.msgTrade.Load(),
		"msg_res", s.msgResolve.Load(),
		"signals", s.signalsTotal.Load(),
		"signals_per_min", fmt.Sprintf("%.2f", sigM),
		"db_feat_batches", s.dbFeatBatch.Load(),
		"db_snap_batches", s.dbSnapBatch.Load(),
		"pending", in.Pending,
		"resolve_queued", s.resolveQueued.Load(),
		"resolve_flushed", s.resolveFlushed.Load(),
		"reconnects", s.reconnects.Load(),
	}
}

// CompactTypes is a short type breakdown for tests.
func (s *RuntimeStats) CompactTypes() string {
	if s == nil {
		return ""
	}
	parts := []string{
		fmt.Sprintf("book=%d", s.msgBook.Load()),
		fmt.Sprintf("pc=%d", s.msgPC.Load()),
		fmt.Sprintf("bba=%d", s.msgBBA.Load()),
		fmt.Sprintf("tr=%d", s.msgTrade.Load()),
		fmt.Sprintf("res=%d", s.msgResolve.Load()),
	}
	return strings.Join(parts, " ")
}
