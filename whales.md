You are a Go expert tasked with building a data sync pipeline for Polymarket trading data. Make use of the current pipeline fetch/process/save architecture to sync accounts > userPositions, and events > markets > enrichedOrderFilleds, orderbooks, pricesHistory, getMarketTradeEvents, spreads:

Fetch: Make API calls or queries to external sources (e.g., HTTP to Gamma/CLOB APIs, GraphQL to subgraph). Use the fetcher to fetch subgraph data and handle pagination, retries, rate limiting (sleep 200-500ms between calls), and errors:4xx/5xx -> retry on specific retryable errors.
Process: Transform fetched data using the processor. Some of the requests will have derivedRequests that need to be fetched. Keep processing lightweight in Go—focus on basic transformations, computations (e.g., collateral_value = size * price, deltas like delta_quantity = current.netQuantity - prev.netQuantity), and flagging potential signals (e.g., whale delta >1000 and book depth > threshold). Avoid complex analytics or machine learning in Go; instead, structure the saved data to be easily consumable by a separate Python agent for advanced analysis (e.g., using pandas for dataframes, numpy for numerical computations, scikit-learn for indicators/models). This ensures accuracy and leverages Python's robust ecosystem, as Go's indicator packages are less comprehensive.
Save: Upsert to TimescaleDB (Postgres extension). Discern hypertables for time-series data (e.g., enriched_order_filled_events, position snapshots—use CREATE HYPERTABLE on timestamp column for chunking/compression). Use regular tables for static data (e.g., markets, accounts). Add indexes for fast queries (e.g., on account_id, market_id, timestamp). Use materialized views for aggregates (e.g., whale volumes). Use pgx or sqlx for DB interactions. Design schema to support easy querying/export to Python (e.g., via SQL dumps or direct DB connection in Python).

The goal is to sync active markets, trades/enrichedOrderFilleds, orderbooks/prices, accounts/whales, and positions for trading signals (e.g., whale tracking). The Go pipeline focuses on reliable data syncing and basic flagging; offload advanced signal generation, indicators, or ML to a separate Python component that reads from the DB. Run as a loop with intervals (discovery every 5min, monitor every 1min).
Constants/Configs:

Subgraph URL: "https://api.thegraph.com/subgraphs/name/.../81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC" (Arbitrum orderbook).
Gamma API: "https://gamma-api.polymarket.com".
CLOB API: "https://clob.polymarket.com".
DB: TimescaleDB connection (assume env vars for DSN).
Whale threshold: collateralVolume > 100000 USD.
Focus on active markets (active=true, closed=false, high volume, high liquidity).

Update DB Schema (create if not exists):
enriched_order_filled_events with hypertable on whatever needs indexed
position_snapshots (hypertable on snapshot_time): snapshot_time (timestamptz), account_id (string), market_id (string), outcomeIndex (int), netQuantity (float), netValue (float), delta_quantity (float, computed in process). Indexes: snapshot_time DESC, account_id, market_id. Materialized view: latest_positions (GROUP BY account_id, market_id).
orderbooks (hypertable on timestamp): timestamp (timestamptz), token_id (string PK with timestamp), bids (jsonb), asks (jsonb), spread (float, computed). Indexes: timestamp DESC, token_id.

Main Loop (run forever):

Discovery Fetch: GET Gamma /events or /markets?active=true&closed=false&order=volume&desc&limit=200. Parse JSON for markets with clobTokenIds.
Process: Filter high-volume/liquidity markets; aggregate all_clob_ids = flatten(markets.clobTokenIds).
Save: Upsert to markets (services.PlyMktMarket) table.
Fills Sync Fetch derived requests: GraphQL enrichedOrderFilleds(where: {market_in: all_clob_ids}, orderBy: timestamp desc, first: 500).
Process: For each enrichedOrderFilledEvent, compute collateral_value = size * price; update account volumes/trades in memory.
Save: Upsert enriched_order_filled_events hypertable; update accounts table with incremented volume/trades ? should I update accounts table ?.
Whale Discovery Process: Query DB for accounts where total_collateral_volume > 100000.
Fetch derived requests: GraphQL userPositions or marketPositions(where: {user_in: whale_ids}).
Process: Compare to previous snapshots, compute deltas (e.g., delta_quantity = current.netQuantity - prev.netQuantity).
Save: Insert position_snapshots hypertable with deltas.
Live Data Fetch (per interesting market/token): Parallel goroutines for /books?token_id=..., /price?token_id=...&side=BUY/SELL, /trades?token_id=... (recent trades as backup to fills).
Process: Compute spread = best_ask - best_bid; aggregate price_history from CLOB_API/prices-history?market={market_id}&startTs={start_ts}&endTs={end_ts}&interval={1m,1w,1d,1h,6h,max}&fidelity={int}. need to correct fidelity for correct range required.
Save: Upsert orderbooks hypertable with spread; update markets with latest volume/liquidity if changed.
Signals/Optimization: In process, flag potential signals (e.g., whale delta >1000 and book depth > threshold) by adding a flag column in the DB (e.g., in position_snapshots). But don't implement trading or advanced analytics—focus on data sync and prepare data for Python-based analysis.
DB Optimizations: After save, refresh materialized views; ensure hypertables for query speed on time ranges.

Additional Requirements for Update Frequency & Logic:

- Discovery phase (Gamma active markets/events): Run every 5 minutes (300 seconds). This refreshes the list of interesting markets and their clobTokenIds.

- Fills sync (enrichedOrderFilleds): Run every 60 seconds (1 minute). This is the primary source of new trade data, account activity, and volume increments. Use incremental queries (e.g., timestamp_gt: last_sync_time) to avoid re-fetching old data.

- Accounts aggregation & whale candidate refresh:
  - Recompute total_collateral_volume and total_trades from fills in the DB after every fills sync.
  - Re-query the top whales from DB (WHERE total_collateral_volume > 100000 ORDER BY total_collateral_volume DESC LIMIT 100) every 2–3 minutes (after 2–3 fills sync cycles).
  - This ensures whale list updates frequently as new high-volume traders appear.

- userPositions / marketPositions snapshots:
  - Only fetch positions for the current top whales (from DB query above).
  - Run this every 3–5 minutes (180–300 seconds), or after every 3 fills sync cycles.
  - Reason: Positions change only when a whale trades (which is captured in fills), so syncing every minute is overkill and rate-limit heavy. Every 3–5 min balances freshness with efficiency.
  - In process step: Compare new snapshot to the previous one in DB → compute delta_quantity, delta_value, direction (buy if positive delta on outcome, sell if negative).
  - Save new snapshot only if changed or at least every 10 minutes (to capture slow drifts or forced refreshes).

- Live CLOB data (orderbook, price, trades): Fetch for markets that have recent fills or active whale positions. Run every 60 seconds (parallel goroutines per token_id).

- Overall loop timing:
  - Outer loop sleep: 60 seconds.
  - Use a ticker or time-based if-checks inside the loop to trigger discovery (every 5 min), whale positions (every 3–5 min), etc.
  - Example:
    if now - last_discovery > 300 { run discovery }
    if now - last_positions_sync > 180 { run whale positions fetch }

- Optimization notes:
  - Cache last_sync_timestamp per component in DB or memory.
  - If no new fills in last cycle → skip expensive positions fetch.
  - Log update frequency and number of whales refreshed each cycle.

Implement in Go with packages: net/http, encoding/json, github.com/machinebox/graphql, github.com/jackc/pgx/v5. Use env for secrets. Handle errors gracefully. Make loop interruptible (context cancel). Output logs for each step.