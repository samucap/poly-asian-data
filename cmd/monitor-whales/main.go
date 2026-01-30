package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
	"golang.org/x/term"
)

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

	// Warm up connection
	if err := db.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	ctx := context.Background()

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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Initial Print
	paused := false
	sortBy := "volume" // Default: volume, profit, trades
	printScreen(ctx, db, paused, sortBy)

	for {
		select {
		case key := <-cmdCh:
			if key == "q" || key == "\x03" { // q or Ctrl+C
				term.Restore(int(os.Stdin.Fd()), oldState)
				fmt.Print("\033[H\033[2J\r")
				fmt.Println("Exiting Whale Monitor.")
				os.Exit(0)
			}
			if key == "p" {
				// If paused, toggle pause. If not paused, check if user meant "Sort by Profit"?
				// Conflict: 'p' is Pause.
				// Let's change keys:
				// [space] = Pause
				// v = Volume
				// p = Profit (We need to re-map Pause)
				// t = Trades
				// OLD: p=Pause
				// NEW: Let's keep p=Pause for muscle memory? But user asked for profit sort.
				// Let's use 's' for Sort (cycle) or specific keys?
				// Let's use:
				//   [space] = Pause/Resume
				//   v = Sort by Volume
				//   f = Sort by Profit (F=Funds?) or 'r' (Return?) or just 'p' and move Pause to Space?
				//   t = Sort by Trades
				//   q = Quit

				// Decision: Move Pause to SPACE. Use 'p' for Profit.
			}

			// Revised Key Handling
			shouldReprint := false
			switch key {
			case "q", "\x03":
				term.Restore(int(os.Stdin.Fd()), oldState)
				fmt.Print("\033[H\033[2J\r")
				fmt.Println("Exiting Whale Monitor.")
				os.Exit(0)
			case " ": // Space to Toggle Pause
				paused = !paused
				shouldReprint = true
			case "v":
				sortBy = "volume"
				shouldReprint = true
			case "p":
				sortBy = "profit"
				shouldReprint = true
			case "t":
				sortBy = "trades"
				shouldReprint = true
			}

			if shouldReprint {
				printScreen(ctx, db, paused, sortBy)
			}

		case <-ticker.C:
			if !paused {
				printScreen(ctx, db, paused, sortBy)
			}
		}
	}
}

// rawWriter wraps an io.Writer and converts \n to \r\n
type rawWriter struct {
	w *os.File
}

func (rw rawWriter) Write(p []byte) (n int, err error) {
	for _, b := range p {
		if b == '\n' {
			if _, err := rw.w.Write([]byte{'\r', '\n'}); err != nil {
				return n, err
			}
		} else {
			if _, err := rw.w.Write([]byte{b}); err != nil {
				return n, err
			}
		}
		n++
	}
	return n, nil
}

func printScreen(ctx context.Context, db *pgxpool.Pool, paused bool, sortBy string) {
	fmt.Print("\033[H\033[2J\r") // Clear screen + carriage return

	w := tabwriter.NewWriter(rawWriter{w: os.Stdout}, 0, 8, 2, ' ', 0)

	status := "LIVE (30s refresh)"
	if paused {
		status = "PAUSED (Press [Space] to resume)"
	}

	fmt.Fprintf(w, "--- WHALE SYNC MONITOR [%s] ---\n", time.Now().Format("15:04:05"))
	fmt.Fprintf(w, "Status: %s\n", status)
	fmt.Fprintf(w, "Sort:   %s\n", sortBy)
	fmt.Fprintf(w, "Keys:   [Space] Pause  [v] Vol  [p] Profit  [t] Trades  [q] Quit\n\n")

	// 1. Account Stats
	var countAccounts int
	var countWhales int
	var avgVol float64

	db.QueryRow(ctx, "SELECT count(*) FROM accounts").Scan(&countAccounts)
	err := db.QueryRow(ctx, `
		SELECT count(*), COALESCE(AVG(a.scaled_collateral_volume::numeric), 0)
		FROM whale_candidates wc
        JOIN accounts a ON wc.account_id = a.id
	`).Scan(&countWhales, &avgVol)

	if err != nil {
		fmt.Fprintf(w, "Error querying stats: %v\n", err)
	} else {
		fmt.Fprintf(w, "Total Accounts:\t%s\n", formatNumber(countAccounts))
		fmt.Fprintf(w, "Whale Candidates (>$100):\t%s\n", formatNumber(countWhales))
		fmt.Fprintf(w, "Avg Whale Vol (Sc):\t%s\n\n", formatMoney(avgVol))
	}

	// 2. Position Snapshots
	var countSnaps int
	var lastSnap time.Time
	err = db.QueryRow(ctx, `
		SELECT count(*), COALESCE(MAX(snapshot_time), '1970-01-01'::timestamptz)
		FROM position_snapshots
	`).Scan(&countSnaps, &lastSnap)
	if err != nil {
		fmt.Fprintf(w, "Error querying snapshots: %v\n", err)
	} else {
		var activeMkts int
		db.QueryRow(ctx, "SELECT count(DISTINCT market_id) FROM position_snapshots WHERE snapshot_time > NOW() - INTERVAL '24 hours'").Scan(&activeMkts)

		fmt.Fprintf(w, "Total Snapshots:\t%d\n", countSnaps)
		fmt.Fprintf(w, "Active Markets (24h):\t%d\n", activeMkts)
		fmt.Fprintf(w, "Last Snapshot:\t%s (%s)\n\n", lastSnap.Format(time.TimeOnly), timeAgo(lastSnap))
	}

	// 3. Top Whales List
	fmt.Fprintf(w, "TOP 10 WHALES (by %s):\n", sortBy)
	fmt.Fprintln(w, "ID\tVolume (Sc)\tProfit (Sc)\tTrades\tLast Active\t")
	fmt.Fprintln(w, "--\t------\t------\t------\t-----------\t")

	// Sort by Scaled or Raw is the same order. Using scaled for consistency if using cast.
	orderBy := "accounts.scaled_collateral_volume::numeric DESC"
	switch sortBy {
	case "profit":
		orderBy = "accounts.scaled_profit::numeric DESC"
	case "trades":
		orderBy = "accounts.num_trades::int DESC"
	}

	query := fmt.Sprintf(`
		SELECT 
			accounts.id,
			COALESCE(accounts.scaled_collateral_volume::numeric, 0),
			COALESCE(accounts.scaled_profit::numeric, 0),
			COALESCE(accounts.num_trades::int, 0),
			accounts.last_traded_timestamp
		FROM whale_candidates 
		JOIN accounts ON whale_candidates.account_id = accounts.id
		ORDER BY %s
		limit 10
	`, orderBy)

	rows, err := db.Query(ctx, query)
	if err != nil {
		fmt.Fprintf(w, "Error querying whales: %v\n", err)
	} else {
		for rows.Next() {
			var id string
			var vol, profit float64
			var trades int
			var lastActiveStr *string
			rows.Scan(&id, &vol, &profit, &trades, &lastActiveStr)
			lastActiveDisplay := "never"
			if lastActiveStr != nil && *lastActiveStr != "" {
				// Parse unix timestamp string
				if ts, err := parseTimestamp(*lastActiveStr); err == nil {
					lastActiveDisplay = timeAgo(ts)
				}
			}
			// Use standard formatting with safe slice
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t\n",
				formatAddress(id), formatMoney(vol), formatMoney(profit), formatNumber(trades), lastActiveDisplay)
		}
		rows.Close()
	}

	// 4. Recent Activity
	fmt.Fprintln(w, "\nRECENT SNAPSHOTS:")
	fmt.Fprintln(w, "Time\tTarget\tMarket\tNet Val\tDelta\t")
	fmt.Fprintln(w, "----\t------\t------\t-------\t-----\t")
	rows, err = db.Query(ctx, `
		SELECT snapshot_time, account_id, market_id, net_value, delta_value
		FROM position_snapshots 
		ORDER BY snapshot_time DESC
		LIMIT 5
	`)
	if err != nil {
		fmt.Fprintf(w, "Error querying snapshots: %v\n", err)
	} else {
		hasRows := false
		for rows.Next() {
			hasRows = true
			var ts time.Time
			var acc, mkt string
			var val, delta float64
			rows.Scan(&ts, &acc, &mkt, &val, &delta)
			fmt.Fprintf(w, "%s\t%s\t%s\t$%.2f\t$%.2f\t\n",
				ts.Format(time.TimeOnly), formatAddress(acc), formatAddress(mkt), val, delta)
		}
		if !hasRows {
			fmt.Fprintln(w, "(no snapshots yet)")
		}
		rows.Close()
	}

	// 5. Whale Flow (Aggregated 24h)
	fmt.Fprintln(w, "\nWHALE FLOW (24h Aggregated):")
	fmt.Fprintln(w, "ID\tMarket\tNet Flow\tActivity\t")
	fmt.Fprintln(w, "--\t------\t--------\t--------\t")

	rows, err = db.Query(ctx, `
		SELECT account_id, market_id, net_flow_usd, activity_count 
		FROM whale_flow_24h 
		ORDER BY ABS(net_flow_usd) DESC 
		LIMIT 5
	`)
	if err != nil {
		fmt.Fprintf(w, "Error querying whale flow: %v\n", err)
	} else {
		hasRows := false
		for rows.Next() {
			hasRows = true
			var acc, mkt string
			var flow float64
			var count int
			rows.Scan(&acc, &mkt, &flow, &count)
			fmt.Fprintf(w, "%s\t%s\t$%.2f\t%d events\t\n",
				formatAddress(acc), formatAddress(mkt), flow, count)
		}
		if !hasRows {
			fmt.Fprintln(w, "(no whale flow in last 24h)")
		}
		rows.Close()
	}

	w.Flush()
}

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

// formatNumber adds commas as thousands separators: 1234567 -> "1,234,567"
func formatNumber(n int) string {
	str := fmt.Sprintf("%d", n)
	if n < 0 {
		str = str[1:] // handle negative
	}
	// Insert commas from the right
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

// formatMoney formats a float as $X,XXX.XX
func formatMoney(n float64) string {
	prefix := "$"
	if n < 0 {
		prefix = "-$"
		n = -n
	}
	intPart := int(n)
	decPart := int((n - float64(intPart)) * 100)
	return fmt.Sprintf("%s%s.%02d", prefix, formatNumber(intPart), decPart)
}

// safeSlice safely slices a string up to n characters, returning the whole string if shorter
func safeSlice(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// formatAddress returns a shortened address like 0x1234...5678
func formatAddress(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:6] + "..." + s[len(s)-4:]
}

// parseTimestamp parses a unix timestamp string to time.Time
func parseTimestamp(s string) (time.Time, error) {
	var ts int64
	if _, err := fmt.Sscanf(s, "%d", &ts); err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0), nil
}
