package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

const (
	timeFormat = "15:04:05.000"

	// ASNI Color Codes
	colorReset   = "\033[0m"
	colorRed     = "\033[31m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
	colorCyan    = "\033[36m"
	colorGray    = "\033[90m"
)

type PrettyHandler struct {
	opts  slog.HandlerOptions
	w     io.Writer
	mu    *sync.Mutex
	attrs []slog.Attr
	group string
}

func NewPrettyHandler(w io.Writer, opts *slog.HandlerOptions) *PrettyHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &PrettyHandler{
		opts: *opts,
		w:    w,
		mu:   &sync.Mutex{},
	}
}

func (h *PrettyHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

func (h *PrettyHandler) Handle(ctx context.Context, r slog.Record) error {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	// 1. Time
	if !r.Time.IsZero() {
		buf.WriteString(colorGray)
		buf.WriteString(r.Time.Format(timeFormat))
		buf.WriteString(colorReset)
		buf.WriteString(" ")
	}

	// 2. Level
	levelColor := colorReset
	switch r.Level {
	case slog.LevelDebug:
		levelColor = colorMagenta
	case slog.LevelInfo:
		levelColor = colorGreen
	case slog.LevelWarn:
		levelColor = colorYellow
	case slog.LevelError:
		levelColor = colorRed
	}

	buf.WriteString(levelColor)
	buf.WriteString(fmt.Sprintf("%-6s", r.Level.String()))
	buf.WriteString(colorReset)
	buf.WriteString(" ")

	// 3. Message
	buf.WriteString(r.Message)

	// 4. Attributes
	// Collect all attrs
	attrs := make([]slog.Attr, 0, len(h.attrs)+r.NumAttrs())
	attrs = append(attrs, h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})

	if len(attrs) > 0 {
		buf.WriteString(" ")
		buf.WriteString(colorGray)
		buf.WriteString("|")
		buf.WriteString(colorReset)
		buf.WriteString(" ")
	}

	for i, a := range attrs {
		if i > 0 {
			buf.WriteString(" ")
		}
		h.appendAttr(buf, a, h.group)
	}

	buf.WriteString("\n")

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(buf.Bytes())
	return err
}

func (h *PrettyHandler) appendAttr(buf *bytes.Buffer, a slog.Attr, groupPrefix string) {
	// Apply ReplaceAttr if present
	if h.opts.ReplaceAttr != nil {
		a = h.opts.ReplaceAttr([]string{}, a)
	}

	if a.Key == "" {
		return
	}

	// Recurse for groups
	if a.Value.Kind() == slog.KindGroup {
		attrs := a.Value.Group()
		if len(attrs) == 0 {
			return
		}
		newPrefix := groupPrefix
		if a.Key != "" {
			if newPrefix != "" {
				newPrefix += "." + a.Key
			} else {
				newPrefix = a.Key
			}
		}
		for i, attr := range attrs {
			if i > 0 {
				buf.WriteString(" ")
			}
			h.appendAttr(buf, attr, newPrefix)
		}
		return
	}

	// Print Key
	buf.WriteString(colorCyan)
	if groupPrefix != "" {
		buf.WriteString(groupPrefix)
		buf.WriteString(".")
	}
	buf.WriteString(a.Key)
	buf.WriteString("=")
	buf.WriteString(colorReset)

	// Print Value
	val := a.Value.Any()
	// Special handling if error to make it red? Or just keep simple.
	// User said "colorize things like field names like component|idk... you get it"
	// User didn't strictly say values need color, but let's see. 
	// For now let's stick to field names colored as requested.

	switch v := val.(type) {
	case string:
		// if it contains spaces, maybe quote it? slog text handler does.
		// but for pretty logs, sometimes raw is nicer.
		// Let's use %q if implies needing quotes, or just %s
		if needsQuote(v) {
			fmt.Fprintf(buf, "%q", v)
		} else {
			buf.WriteString(v)
		}
	case error:
		buf.WriteString(colorRed)
		buf.WriteString(v.Error())
		buf.WriteString(colorReset)
	default:
		// Use JSON encoding for complex types or fmt.Sprint for simple ones
		b, err := json.Marshal(v)
		if err == nil {
			buf.Write(b)
		} else {
			fmt.Fprint(buf, v)
		}
	}
}

func needsQuote(s string) bool {
	for _, r := range s {
		if r <= ' ' || r == '=' || r == '"' {
			return true
		}
	}
	return s == ""
}

func (h *PrettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)

	return &PrettyHandler{
		opts:  h.opts,
		w:     h.w,
		mu:    h.mu, // shared mutex for writing to same writer
		attrs: newAttrs,
		group: h.group,
	}
}

func (h *PrettyHandler) WithGroup(name string) slog.Handler {
	newGroup := h.group
	if newGroup != "" {
		newGroup += "." + name
	} else {
		newGroup = name
	}

	return &PrettyHandler{
		opts:  h.opts,
		w:     h.w,
		mu:    h.mu,
		attrs: h.attrs,
		group: newGroup,
	}
}
