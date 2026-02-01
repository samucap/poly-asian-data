package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
	"golang.org/x/term"
)

// =============================================================================
// Styling & Constants
// =============================================================================

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
	ColorGray   = "\033[90m"

	Bold      = "\033[1m"
	Underline = "\033[4m"
)

type SyncStatus struct {
	Type       string
	Cursor     string
	LastSync   time.Time
	TotalItems int
	Status     string
	Rate       float64 // items/sec (calculated)
}

type AppState struct {
	syncStats map[string]SyncStatus
	mu        sync.RWMutex
}

func main() {
	// 1. Setup
	os.Setenv("ENV", "dev")
	logging.Init("dev")
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	db, err := pgxpool.New(context.Background(), cfg.PostgresURL)
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	defer db.Close()

	// Warm up
	if err := db.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	ctx := context.Background()
	state := &AppState{
		syncStats: make(map[string]SyncStatus),
	}

	// 2. Enable Raw Mode
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		log.Fatalf("failed to set raw mode: %v", err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	// 3. Input Channel
	cmdCh := make(chan string)
	go func() {
		b := make([]byte, 1)
		for {
			os.Stdin.Read(b)
			cmdCh <- string(b)
		}
	}()

	// 4. Main Event Loop
	ticker := time.NewTicker(2 * time.Second) // Fast refresh
	defer ticker.Stop()

	// Initial Print
	paused := false
	sortBy := "volume"
	updateSyncStats(ctx, db, state)
	printScreen(ctx, db, state, paused, sortBy)

	for {
		select {
		case key := <-cmdCh:
			switch key {
			case "q", "\x03":
				term.Restore(int(os.Stdin.Fd()), oldState)
				fmt.Print("\033[H\033[2J\r")
				fmt.Println("Exiting Whale Monitor.")
				os.Exit(0)
			case " ":
				paused = !paused
			case "v":
				sortBy = "volume"
			case "p":
				sortBy = "profit"
			case "t":
				sortBy = "trades"
			}
			printScreen(ctx, db, state, paused, sortBy)

		case <-ticker.C:
			if !paused {
				updateSyncStats(ctx, db, state)
				printScreen(ctx, db, state, paused, sortBy)
			}
		}
	}
}

// updateSyncStats fetches sync_state and calculates rates
func updateSyncStats(ctx context.Context, db *pgxpool.Pool, state *AppState) {
	rows, err := db.Query(ctx, "SELECT sync_type, last_cursor, last_sync_at, total_items, status FROM sync_state")
	if err != nil {
		return
	}
	defer rows.Close()

	state.mu.Lock()
	defer state.mu.Unlock()


	for rows.Next() {
		var s SyncStatus
		var cursorPtr *string
		if err := rows.Scan(&s.Type, &cursorPtr, &s.LastSync, &s.TotalItems, &s.Status); err != nil {
			continue
		}
		if cursorPtr != nil {
			s.Cursor = *cursorPtr
		}

		// Calculate rate (items/sec) based on previous state
		if prev, ok := state.syncStats[s.Type]; ok {
			deltaItems := float64(s.TotalItems - prev.TotalItems)
			// deltaTime := now.Sub(prev.LastSync).Seconds() // Unused, using fixed 2s interval
			// Better: store fetch time in state?
			// Let's just use simple diff for now, assuming 2s interval
			if deltaItems > 0 && deltaItems < 10000 { // Ignore massive jumps (initial load)
				s.Rate = deltaItems / 2.0 // Approx
			}
		}

		state.syncStats[s.Type] = s
	}
}

func printScreen(ctx context.Context, db *pgxpool.Pool, state *AppState, paused bool, sortBy string) {
	// Buffer output to avoid flickering
	var b strings.Builder
	
	// Clear screen
	b.WriteString("\033[H\033[2J\r")

	w := tabwriter.NewWriter(&b, 0, 8, 2, ' ', 0)

	// Header
	refreshStatus := fmt.Sprintf("%sON%s (2s)", ColorGreen, ColorReset)
	if paused {
		refreshStatus = fmt.Sprintf("%sPAUSED%s", ColorYellow, ColorReset)
	}

	// Calculate Global System Status
	systemStatus := fmt.Sprintf("%sIDLE/OFFLINE%s", ColorRed, ColorReset)
	activeCount := 0
	state.mu.RLock()
	for _, s := range state.syncStats {
		if strings.ToLower(s.Status) == "running" && time.Since(s.LastSync) < 20*time.Second {
			activeCount++
		}
	}
	state.mu.RUnlock()
	
	if activeCount > 0 {
		systemStatus = fmt.Sprintf("%sSYNCING (%d services)%s", ColorGreen, activeCount, ColorReset)
	}

	fmt.Fprintf(w, "%s=== 🐋 POLYMARKET WHALE MONITOR ===%s\n", Bold+ColorCyan, ColorReset)
	fmt.Fprintf(w, "System: %s  |  Refresh: %s  |  Sort: %s%s%s\n", 
		systemStatus, refreshStatus, ColorBlue, sortBy, ColorReset)
	fmt.Fprintf(w, "Keys: [Space] Pause [v] Vol [p] Profit [t] Trades [q] Quit\n\n")

	// 1. SYNC STATUS
	fmt.Fprintf(w, "%s🚀 SYNC PULSE%s\n", Bold+ColorPurple, ColorReset)
	fmt.Fprintf(w, "Service\tStatus\tLag\tEvents/s\tTotal\tCursor\t\n")
	fmt.Fprintf(w, "-------\t------\t---\t--------\t-----\t------\t\n")

	// Grouping
	groups := map[string][]string{
		"Infrastructure": {"accounts", "sports", "leagues", "teams", "tags_definitions", "tags_sport_link", "tags_hierarchy", "league_hierarchy"},
		"Live Data":      {"position_snapshots", "orderbooks", "prices_history", "enriched_order_filled_events", "plymkt_events", "plymkt_markets", "conditions"},
	}

	state.mu.RLock()
	// Helper to print a group
	printGroup := func(name string, types []string) {
		first := true
		for _, t := range types {
			if s, ok := state.syncStats[t]; ok {
				if first {
					// fmt.Fprintf(w, "%s%s:%s\t\t\t\t\t\t\n", Bold, name, ColorReset)
					// Actually integrated list looks cleaner
					first = false
				}
				
				// Status Logic with Heartbeat
				statusText := s.Status
				statusColor := ColorGreen
				
				// Stale Check: If status says 'running' but lag > 20s, it's blocked/stalled
				if strings.ToLower(s.Status) == "running" && time.Since(s.LastSync) > 20*time.Second {
					statusText = "STALLED"
					statusColor = ColorRed
				} else if strings.ToLower(s.Status) != "running" && strings.ToLower(s.Status) != "completed" {
					statusColor = ColorRed
				}

				lag := time.Since(s.LastSync).Round(time.Second)
				lagStr := fmt.Sprintf("%s", lag)
				if lag < time.Minute {
					lagStr = fmt.Sprintf("%s%s%s", ColorGreen, lagStr, ColorReset)
				} else {
					lagStr = fmt.Sprintf("%s%s%s", ColorRed, lagStr, ColorReset)
				}

				rateStr := "-"
				if s.Rate > 0 {
					rateStr = fmt.Sprintf("%.1f", s.Rate)
				}

				cursorShort := safeSlice(s.Cursor, 20)
				if len(s.Cursor) > 20 { cursorShort += "..." }

				fmt.Fprintf(w, "%s\t%s%s%s\t%s\t%s\t%d\t%s\t\n", 
					t, statusColor, safeSlice(statusText, 8), ColorReset, lagStr, rateStr, s.TotalItems, ColorGray+cursorShort+ColorReset)
			}
		}
	}

	printGroup("Live Data", groups["Live Data"])
	fmt.Fprintln(w, "") // Spacer
	printGroup("Infrastructure", groups["Infrastructure"])
	state.mu.RUnlock()
	fmt.Fprintln(w, "")


	// 2. WHALE LEADERBOARD
	fmt.Fprintf(w, "%s👑 WHALE LEADERBOARD (Top 5)%s\n", Bold+ColorYellow, ColorReset)
	fmt.Fprintln(w, "Address\tVol (Sc)\tProfit (Sc)\tTrades\tLast Active\t")
	fmt.Fprintln(w, "-------\t--------\t-----------\t------\t-----------\t")

	orderBy := "scaled_collateral_volume::numeric DESC"
	switch sortBy {
	case "profit":
		orderBy = "scaled_profit::numeric DESC"
	case "trades":
		orderBy = "num_trades::int DESC"
	}

	// Query accounts directly to avoid view sync issues or complexity
	// Also ensure we are picking up valid numbers.
	query := fmt.Sprintf(`
		SELECT 
			id,
			COALESCE(scaled_collateral_volume::numeric, 0),
			COALESCE(scaled_profit::numeric, 0),
			COALESCE(num_trades::int, 0),
			last_traded_timestamp
		FROM accounts
		WHERE scaled_collateral_volume IS NOT NULL 
		  AND (scaled_collateral_volume::numeric) > 100
		ORDER BY %s
		LIMIT 5
	`, orderBy)

	rows, err := db.Query(ctx, query)
	if err != nil {
		fmt.Fprintf(w, "Error: %v\n", err)
	} else {
		for rows.Next() {
			var id string
			var vol, profit float64
			var trades int
			var lastActive *time.Time
			
			// Scan must match SELECT columns exactly
			if err := rows.Scan(&id, &vol, &profit, &trades, &lastActive); err != nil {
				fmt.Fprintf(w, "Scan Error: %v\n", err)
				continue
			}
			
			lastActiveDisplay := "never"
			if lastActive != nil {
				// User requested DateTime with TZ
				lastActiveDisplay = lastActive.Format("2006-01-02 15:04 MST")
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t\n",
				formatAddress(id), formatMoney(vol), formatMoney(profit), trades, lastActiveDisplay)
		}
		rows.Close()
	}
	fmt.Fprintln(w, "")

	// 3. WHALE FILLS (New)
	fmt.Fprintf(w, "%s⚡ RECENT WHALE FILLS%s\n", Bold+ColorBlue, ColorReset)
	fmt.Fprintln(w, "Time\tMarket\tWhale\tSide\tSize\tPrice\tValue\t")
	fmt.Fprintln(w, "----\t------\t-----\t----\t----\t-----\t-----\t")
	
	// Complex join to get readable data
	// Limiting to recent 5
	fillsQuery := `
		SELECT 
			e.timestamp,
			e.market_id,
			e.maker_id,
			e.side,
			e.size,
			e.price
		FROM enriched_order_filled_events e
		WHERE e.size::numeric > 100 -- Only show larger fills
		ORDER BY e.timestamp DESC
		LIMIT 5
	`
	rows, err = db.Query(ctx, fillsQuery)
	if err != nil {
		fmt.Fprintf(w, "Error: %v\n", err)
	} else {
		for rows.Next() {
			var ts time.Time
			var mkt, whale, side, size, price string
			rows.Scan(&ts, &mkt, &whale, &side, &size, &price)
			
			// Colorize Side
			sideColor := ColorGreen
			if strings.ToUpper(side) == "SELL" {
				sideColor = ColorRed
			}

			// Format Size and Price pretty
			var sizeVal, priceVal float64
			fmt.Sscanf(size, "%f", &sizeVal)
			fmt.Sscanf(price, "%f", &priceVal)
			
			valDisplay := formatMoney(sizeVal * priceVal)

			fmt.Fprintf(w, "%s\t%s\t%s\t%s%s%s\t%s\t%s\t%s\t\n",
				ts.Format("15:04:05"), formatAddress(mkt), formatAddress(whale), 
				sideColor, side, ColorReset, formatNumber(int(sizeVal)), fmt.Sprintf("$%.2f", priceVal), valDisplay)
		}
		rows.Close()
	}
	fmt.Fprintln(w, "")

	// 4. ACTIVITY BY SPORT (New)
	fmt.Fprintf(w, "%s📊 ACTIVITY BY CATEGORY (24h)%s\n", Bold+ColorCyan, ColorReset)
	fmt.Fprintln(w, "Category\tVolume\tEvents\t")
	fmt.Fprintln(w, "--------\t------\t------\t")

	catQuery := `
		SELECT 
			COALESCE(pm.category, 'Unknown'),
			SUM(e.size::numeric * e.price::numeric) as vol,
			COUNT(*)
		FROM enriched_order_filled_events e
		LEFT JOIN plymkt_markets pm ON e.market_id = pm.id
		WHERE e.timestamp > NOW() - INTERVAL '24 hours'
		GROUP BY pm.category
		ORDER BY vol DESC
		LIMIT 5
	`
	rows, err = db.Query(ctx, catQuery)
	if err != nil {
		fmt.Fprintf(w, "Error: %v\n", err)
	} else {
		for rows.Next() {
			var cat string
			var vol float64
			var count int
			rows.Scan(&cat, &vol, &count)
			fmt.Fprintf(w, "%s\t%s\t%d\t\n", safeSlice(cat, 15), formatMoney(vol), count)
		}
		rows.Close()
	}


	w.Flush()
	
	// Write buffer to stdout
	// We use rawWriter in main loop? No, tabwriter handles it if we pass os.Stdout directly?
	// But we need to handle \n -> \r\n conversion if raw mode.
	// Let's print the string with manual conversion.
	out := b.String()
	// Replace \n with \r\n for raw mode
	out = strings.ReplaceAll(out, "\n", "\r\n")
	os.Stdout.WriteString(out)
}

// Helpers

func timeAgo(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := time.Since(t)
	if d < time.Minute {
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}

func formatNumber(n int) string {
	str := fmt.Sprintf("%d", n)
	if n < 0 {
		str = str[1:]
	}
	var result []byte
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	if n < 0 {
		return "-" + string(result)
	}
	return string(result)
}

func formatMoney(n float64) string {
	prefix := "$"
	if n < 0 {
		prefix = "-$"
		n = -n
	}
	if n >= 1000000 {
		return fmt.Sprintf("%s%sM", prefix, formatNumber(int(n/1000000)))
	}
	if n >= 1000 {
		return fmt.Sprintf("%s%sk", prefix, formatNumber(int(n/1000)))
	}
	return fmt.Sprintf("%s%.2f", prefix, n) // Simplified for space
}

func safeSlice(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func formatAddress(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:6] + ".." + s[len(s)-4:]
}

func parseTimestamp(s string) (time.Time, error) {
	var ts int64
	if _, err := fmt.Sscanf(s, "%d", &ts); err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0), nil
}
