package market

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Client is a market-channel WS client with dynamic subscribe and PING.
type Client struct {
	URL            string
	PingEvery      time.Duration
	Logger         *slog.Logger
	OnEvent        func(ParsedEvent)
	OnReconnect    func()
	ReconnectMin   time.Duration
	ReconnectMax   time.Duration
	CustomFeatures bool

	mu          sync.Mutex
	conn        *websocket.Conn
	subscribed  []string
	desired     []string
	connected   bool
}

// NewClient returns a client with defaults.
func NewClient(url string, log *slog.Logger) *Client {
	if url == "" {
		url = DefaultWSURL
	}
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		URL:            url,
		PingEvery:      DefaultPingEvery,
		Logger:         log,
		ReconnectMin:   time.Second,
		ReconnectMax:   30 * time.Second,
		CustomFeatures: true,
	}
}

// SetDesired updates the desired asset set and applies diff if connected.
func (c *Client) SetDesired(ctx context.Context, assets []string) error {
	c.mu.Lock()
	c.desired = append([]string(nil), assets...)
	conn := c.conn
	cur := append([]string(nil), c.subscribed...)
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	diff := DiffSubscriptions(cur, assets)
	if err := c.applyDiff(ctx, conn, diff); err != nil {
		return err
	}
	c.mu.Lock()
	c.subscribed = append([]string(nil), assets...)
	c.mu.Unlock()
	return nil
}

// Subscribed returns a copy of currently subscribed asset ids.
func (c *Client) Subscribed() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.subscribed...)
}

// RemoveAssets drops assets from desired set and unsubscribes if connected (lean resolve path).
func (c *Client) RemoveAssets(ctx context.Context, assets []string) error {
	if c == nil || len(assets) == 0 {
		return nil
	}
	drop := make(map[string]struct{}, len(assets))
	for _, a := range assets {
		if a != "" {
			drop[a] = struct{}{}
		}
	}
	c.mu.Lock()
	var next []string
	for _, a := range c.desired {
		if _, bad := drop[a]; !bad {
			next = append(next, a)
		}
	}
	c.desired = next
	conn := c.conn
	cur := append([]string(nil), c.subscribed...)
	c.mu.Unlock()

	if conn == nil {
		c.mu.Lock()
		// also trim subscribed snapshot for gauges
		var sub []string
		for _, a := range cur {
			if _, bad := drop[a]; !bad {
				sub = append(sub, a)
			}
		}
		c.subscribed = sub
		c.mu.Unlock()
		return nil
	}
	// Apply full desired diff
	return c.SetDesired(ctx, next)
}

// Run dials, subscribes, reads until ctx cancel. Reconnects with backoff.
func (c *Client) Run(ctx context.Context) error {
	backoff := c.ReconnectMin
	if backoff <= 0 {
		backoff = time.Second
	}
	maxBO := c.ReconnectMax
	if maxBO <= 0 {
		maxBO = 30 * time.Second
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := c.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.Logger.Warn("ws session ended; reconnecting", "error", err, "backoff", backoff)
		// Caller may attach reconnect counter via Logger only; optional hook:
		if c.OnReconnect != nil {
			c.OnReconnect()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBO {
			backoff = maxBO
		}
	}
}

func (c *Client) session(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, c.URL, &websocket.DialOptions{
		HTTPHeader: http.Header{},
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(8 << 20)

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	desired := append([]string(nil), c.desired...)
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.connected = false
		c.subscribed = nil
		c.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "session end")
	}()

	if len(desired) > 0 {
		if err := c.sendInitialSubscribe(ctx, conn, desired); err != nil {
			return err
		}
		c.mu.Lock()
		c.subscribed = append([]string(nil), desired...)
		c.mu.Unlock()
	}

	// Ping loop
	pingEvery := c.PingEvery
	if pingEvery <= 0 {
		pingEvery = DefaultPingEvery
	}
	pingCtx, pingCancel := context.WithCancel(ctx)
	defer pingCancel()
	go func() {
		t := time.NewTicker(pingEvery)
		defer t.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-t.C:
				wctx, cxl := context.WithTimeout(pingCtx, 5*time.Second)
				err := conn.Write(wctx, websocket.MessageText, []byte("PING"))
				cxl()
				if err != nil {
					c.Logger.Debug("ping failed", "error", err)
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		evs, err := ParseMessage(data)
		if err != nil {
			c.Logger.Debug("parse ws message", "error", err)
			continue
		}
		for _, ev := range evs {
			if c.OnEvent != nil {
				c.OnEvent(ev)
			}
		}
	}
}

func (c *Client) sendInitialSubscribe(ctx context.Context, conn *websocket.Conn, assets []string) error {
	payload := map[string]any{
		"assets_ids": assets,
		"type":       "market",
	}
	if c.CustomFeatures {
		payload["custom_feature_enabled"] = true
	}
	return writeJSON(ctx, conn, payload)
}

func (c *Client) applyDiff(ctx context.Context, conn *websocket.Conn, diff SubDiff) error {
	if len(diff.Unsubscribe) > 0 {
		payload := map[string]any{
			"assets_ids": diff.Unsubscribe,
			"operation":  "unsubscribe",
		}
		if err := writeJSON(ctx, conn, payload); err != nil {
			return err
		}
	}
	if len(diff.Subscribe) > 0 {
		payload := map[string]any{
			"assets_ids": diff.Subscribe,
			"operation":  "subscribe",
		}
		if c.CustomFeatures {
			payload["custom_feature_enabled"] = true
		}
		if err := writeJSON(ctx, conn, payload); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, b)
}
