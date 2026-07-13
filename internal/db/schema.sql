-- 0. Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Function for auto-updating updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 2. Tables
CREATE TABLE IF NOT EXISTS sports (
    id UUID DEFAULT gen_random_uuid() UNIQUE,
    slug TEXT PRIMARY KEY NOT NULL,
    primary_tag_id TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tags (
    id TEXT PRIMARY KEY,
    label TEXT,
    slug TEXT,
    force_show BOOLEAN,
    force_hide BOOLEAN,
    sport_id UUID REFERENCES sports(id),
    parent_tag_id TEXT REFERENCES tags(id) DEFERRABLE INITIALLY DEFERRED,
    total_vol DOUBLE PRECISION DEFAULT 0,
    total_vol_24hr DOUBLE PRECISION DEFAULT 0,
    total_liq DOUBLE PRECISION DEFAULT 0,
    total_markets INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Deferrable Constraint to allow circular references during transaction
-- Ensure any existing sports.primary_tag_id exist in tags so ADD CONSTRAINT does not fail
INSERT INTO tags (id)
SELECT DISTINCT primary_tag_id FROM sports
WHERE primary_tag_id IS NOT NULL AND primary_tag_id <> ''
ON CONFLICT (id) DO NOTHING;

ALTER TABLE sports DROP CONSTRAINT IF EXISTS fk_sports_primary_tag;
ALTER TABLE sports ADD CONSTRAINT fk_sports_primary_tag 
    FOREIGN KEY (primary_tag_id) REFERENCES tags(id) 
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS leagues (
    sport TEXT PRIMARY KEY,
    image TEXT,
    resolution TEXT,
    ordering TEXT,
    raw_tags TEXT,
    series TEXT,
    sport_id UUID REFERENCES sports(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS teams (
    id INT PRIMARY KEY,
    name TEXT,
    league TEXT,
    record TEXT,
    logo TEXT,
    abbreviation TEXT,
    alias TEXT,
    provider_id INT,
    color TEXT,
    sport_id UUID REFERENCES sports(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conditions (
    id TEXT PRIMARY KEY,
    oracle TEXT,
    outcome_slot_count TEXT, -- INT or TEXT, using TEXT for big.Int safety
    payout_denominator TEXT,
    payout_numerators TEXT[], -- Array of BigInt/Text
    payouts TEXT[],
    question_id TEXT,
    resolution_hash TEXT,
    resolution_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY, -- Address
    creation_timestamp TIMESTAMPTZ,
    last_seen_timestamp TIMESTAMPTZ,
    last_traded_timestamp TIMESTAMPTZ,
    collateral_volume TEXT,
    num_trades TEXT,
    profit TEXT,
    scaled_collateral_volume TEXT,
    scaled_profit TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_filled_events (
    id TEXT PRIMARY KEY, -- transaction hash or unique ID
    maker_asset_id TEXT,
    taker_asset_id TEXT,
    maker_amount_filled TEXT,
    taker_amount_filled TEXT,
    maker_id TEXT,
    taker_id TEXT,
    fee TEXT,
    timestamp TIMESTAMPTZ,
    transaction_hash TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS enriched_order_filled_events (
    id TEXT NOT NULL, -- transaction hash
    price TEXT,
    side TEXT,
    size TEXT,
    maker_id TEXT,
    taker_id TEXT,
    market_id TEXT,
    timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, timestamp)
);

-- Events from Gamma API /events endpoint
CREATE TABLE IF NOT EXISTS plymkt_events (
    id TEXT PRIMARY KEY,
    ticker TEXT,
    slug TEXT,
    title TEXT,
    description TEXT,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    category TEXT,
    image TEXT,
    icon TEXT,
    active BOOLEAN,
    closed BOOLEAN,
    archived BOOLEAN,
    new BOOLEAN,
    featured BOOLEAN,
    restricted BOOLEAN,
    liquidity NUMERIC,
    volume NUMERIC,
    volume_24hr NUMERIC,
    volume_1wk NUMERIC,
    volume_1mo NUMERIC,
    volume_1yr NUMERIC,
    liquidity_clob NUMERIC,
    competitive NUMERIC,
    neg_risk BOOLEAN,
    neg_risk_market_id TEXT,
    comment_count INT,
    enable_order_book BOOLEAN,
    series_slug TEXT,
    live BOOLEAN,
    ended BOOLEAN,
    creator_id TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS plymkt_markets (
    id TEXT PRIMARY KEY,
    question TEXT,
    condition_id TEXT,
    slug TEXT,
    twitter_card_image TEXT,
    resolution_source TEXT,
    end_date TIMESTAMPTZ,
    category TEXT,
    amm_type TEXT,
    liquidity TEXT,
    sponsor_name TEXT,
    sponsor_image TEXT,
    start_date TIMESTAMPTZ,
    x_axis_value TEXT,
    y_axis_value TEXT,
    denomination_token TEXT,
    image TEXT,
    icon TEXT,
    lower_bound TEXT,
    upper_bound TEXT,
    description TEXT,
    outcomes TEXT,
    outcome_prices TEXT,
    volume TEXT,
    active BOOLEAN,
    market_type TEXT,
    format_type TEXT,
    lower_bound_date TEXT,
    upper_bound_date TEXT,
    closed BOOLEAN,
    market_maker_address TEXT,
    created_by BIGINT,
    updated_by BIGINT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    closed_time TEXT,
    wide_format BOOLEAN,
    new BOOLEAN,
    mailchimp_tag TEXT,
    featured BOOLEAN,
    archived BOOLEAN,
    resolved_by TEXT,
    restricted BOOLEAN,
    market_group BIGINT,
    group_item_title TEXT,
    group_item_threshold TEXT,
    question_id TEXT,
    uma_end_date TEXT,
    enable_order_book BOOLEAN,
    order_price_min_tick_size BIGINT,
    order_min_size BIGINT,
    uma_resolution_status TEXT,
    curation_order BIGINT,
    volume_num NUMERIC,
    liquidity_num NUMERIC,
    end_date_iso TEXT,
    start_date_iso TEXT,
    uma_end_date_iso TEXT,
    has_reviewed_dates BOOLEAN,
    ready_for_cron BOOLEAN,
    comments_enabled BOOLEAN,
    volume_24hr NUMERIC,
    volume_1wk NUMERIC,
    volume_1mo NUMERIC,
    volume_1yr NUMERIC,
    game_start_time TEXT,
    seconds_delay BIGINT,
    clob_token_ids TEXT,
    disqus_thread TEXT,
    short_outcomes TEXT,
    team_a_id TEXT,
    team_b_id TEXT,
    uma_bond TEXT,
    uma_reward TEXT,
    fpmm_live BOOLEAN,
    volume_24hr_amm NUMERIC,
    volume_1wk_amm NUMERIC,
    volume_1mo_amm NUMERIC,
    volume_1yr_amm NUMERIC,
    volume_24hr_clob NUMERIC,
    volume_1wk_clob NUMERIC,
    volume_1mo_clob NUMERIC,
    volume_1yr_clob NUMERIC,
    volume_amm NUMERIC,
    volume_clob NUMERIC,
    liquidity_amm NUMERIC,
    liquidity_clob NUMERIC,
    maker_base_fee BIGINT,
    taker_base_fee BIGINT,
    custom_liveness BIGINT,
    accepting_orders BOOLEAN,
    notifications_enabled BOOLEAN,
    score BIGINT,
    -- optimized fields
    image_optimized_id TEXT,
    image_optimized_url_source TEXT,
    image_optimized_url_optimized TEXT,
    icon_optimized_id TEXT,
    icon_optimized_url_source TEXT,
    icon_optimized_url_optimized TEXT,
    creator TEXT,
    ready BOOLEAN,
    funded BOOLEAN,
    past_slugs TEXT,
    ready_timestamp TIMESTAMPTZ,
    funded_timestamp TIMESTAMPTZ,
    accepting_orders_timestamp TIMESTAMPTZ,
    competitive NUMERIC,
    rewards_min_size NUMERIC,
    rewards_max_spread NUMERIC,
    automatically_resolved BOOLEAN,
    one_day_price_change NUMERIC,
    one_hour_price_change NUMERIC,
    one_week_price_change NUMERIC,
    one_month_price_change NUMERIC,
    one_year_price_change NUMERIC,
    last_trade_price NUMERIC,
    best_bid NUMERIC,
    best_ask NUMERIC,
    automatically_active BOOLEAN,
    clear_book_on_start BOOLEAN,
    chart_color TEXT,
    series_color TEXT,
    show_gmp_series BOOLEAN,
    show_gmp_outcome BOOLEAN,
    manual_activation BOOLEAN,
    neg_risk_other BOOLEAN,
    game_id TEXT,
    group_item_range TEXT,
    sports_market_type TEXT,
    line BIGINT,
    uma_resolution_statuses TEXT,
    pending_deployment BOOLEAN,
    deploying BOOLEAN,
    deploying_timestamp TIMESTAMPTZ,
    scheduled_deployment_timestamp TIMESTAMPTZ,
    rfq_enabled BOOLEAN,
    event_start_time TIMESTAMPTZ,
    raw_json JSONB,
    oi DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    fee DOUBLE PRECISION
);

-- 3. Triggers
DROP TRIGGER IF EXISTS update_sports_updated_at ON sports;
CREATE TRIGGER update_sports_updated_at BEFORE UPDATE ON sports FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_tags_updated_at ON tags;
CREATE TRIGGER update_tags_updated_at BEFORE UPDATE ON tags FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_leagues_updated_at ON leagues;
CREATE TRIGGER update_leagues_updated_at BEFORE UPDATE ON leagues FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


DROP TRIGGER IF EXISTS update_teams_updated_at ON teams;
CREATE TRIGGER update_teams_updated_at BEFORE UPDATE ON teams FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- New Tables for Whale Tracking Pipeline

-- Position Snapshots (Hypertable)
-- Position Snapshots (Hypertable)

CREATE TABLE IF NOT EXISTS position_snapshots (
    snapshot_time TIMESTAMPTZ NOT NULL,
    account_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome_index INT,
    net_quantity DOUBLE PRECISION,
    net_value DOUBLE PRECISION,
    delta_quantity DOUBLE PRECISION,
    delta_value DOUBLE PRECISION,
    is_signal BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
-- Create Hypertable (after table creation)
-- SELECT create_hypertable('position_snapshots', 'snapshot_time', if_not_exists => TRUE);
-- Note: User might run this manually or via migration. We include it here as comment or executable if rights allow.
-- For standard timescaledb setup, we usually run:
SELECT create_hypertable('position_snapshots', 'snapshot_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_position_snapshots_account ON position_snapshots (account_id, snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_position_snapshots_market ON position_snapshots (market_id, snapshot_time DESC);


-- Orderbooks (Hypertable)
-- Orderbooks (Hypertable)

CREATE TABLE IF NOT EXISTS orderbooks (
    timestamp TIMESTAMPTZ NOT NULL,
    token_id TEXT NOT NULL,
    bids JSONB,
    asks JSONB,
    spread DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
SELECT create_hypertable('orderbooks', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_orderbooks_token_ts ON orderbooks (token_id, timestamp DESC);



-- Prices History (Hypertable)
CREATE TABLE IF NOT EXISTS prices_history (
    token_id     TEXT             NOT NULL,
    timestamp    BIGINT           NOT NULL,
    price        DOUBLE PRECISION NOT NULL,
    market_id    TEXT,
    fidelity_min INTEGER,
    updated_at   BIGINT,
    PRIMARY KEY (token_id, timestamp)
);

SELECT create_hypertable('prices_history', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);
-- Index is implicit with Primary Key composite, but we might want just timestamp for general queries?
-- Access pattern: usually by token_id per time range.
-- PK covers (token_id, timestamp).
-- Add index for global time queries/cleanup if needed?
-- User requested: CREATE INDEX ON prices_history (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_prices_history_timestamp ON prices_history (timestamp DESC);


-- Enriched Order Filled Events (Ensure Hypertable)
-- We already defined this table, but let's ensure it's a hypertable.
-- In standard Postgres, if data exists, converting might require migration steps.
-- Assuming fresh start or compatible:
SELECT create_hypertable('enriched_order_filled_events', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);

-- Materialized Views

-- Latest Positions (for quick lookup of current state)
-- Latest Positions (for quick lookup of current state)
-- Converted to Standard View for Real-time Access
CREATE OR REPLACE VIEW latest_positions AS
SELECT DISTINCT ON (account_id, market_id, outcome_index)
    snapshot_time,
    account_id,
    market_id,
    outcome_index,
    net_quantity,
    net_value
FROM position_snapshots
ORDER BY account_id, market_id, outcome_index, snapshot_time DESC;

-- CREATE INDEX IF NOT EXISTS idx_latest_positions_lookup ON latest_positions (account_id, market_id);

-- Whale Candidates (Top volume accounts)
-- Whale Candidates (Top volume accounts)
-- Converted to Standard View for Real-time Access
CREATE OR REPLACE VIEW whale_candidates AS
SELECT
    id AS account_id,
    scaled_collateral_volume,
    num_trades
FROM accounts
WHERE scaled_collateral_volume IS NOT NULL 
  AND (scaled_collateral_volume::NUMERIC) > 100  -- $100 USD threshold
ORDER BY scaled_collateral_volume::NUMERIC DESC;

-- Standard Views do not have independent indexes (they use underlying table)
-- CREATE UNIQUE INDEX IF NOT EXISTS idx_whale_candidates_id ON whale_candidates (account_id);

-- =============================================================================
-- Feature Store: SQL-Based Indicators (TimescaleDB Continuous Aggregates)
-- =============================================================================

-- 1. Candlesticks (Price OHLCV) - Hourly aggregation of prices_history
-- Note: Continuous Aggregates require hypertable. prices_history should be one.
-- CREATE MATERIALIZED VIEW candlesticks_1h
-- WITH (timescaledb.continuous) AS
-- SELECT
--     time_bucket('1 hour', timestamp) AS bucket,
--     token_id,
--     first(price, timestamp) AS open,
--     max(price) AS high,
--     min(price) AS low,
--     last(price, timestamp) AS close,
--     avg(price) AS vwap,
--     count(*) AS trades
-- FROM prices_history
-- GROUP BY bucket, token_id
-- WITH NO DATA;

-- 2. Market Pressure (Buy/Sell Ratio from Fills) - Hourly
CREATE MATERIALIZED VIEW IF NOT EXISTS market_pressure_1h AS
SELECT
    date_trunc('hour', timestamp::timestamp) AS bucket,
    market_id,
    SUM(CASE WHEN side = 'BUY' THEN size::NUMERIC ELSE 0 END) AS buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN size::NUMERIC ELSE 0 END) AS sell_volume,
    CASE 
        WHEN SUM(size::NUMERIC) > 0 THEN 
            SUM(CASE WHEN side = 'BUY' THEN size::NUMERIC ELSE 0 END) / SUM(size::NUMERIC)
        ELSE 0.5 
    END AS buy_ratio
FROM enriched_order_filled_events
GROUP BY bucket, market_id;

CREATE INDEX IF NOT EXISTS idx_market_pressure_lookup ON market_pressure_1h (market_id, bucket DESC);

-- 3. Whale Rankings (Top accounts by 7-day volume)
CREATE MATERIALIZED VIEW IF NOT EXISTS whale_rankings AS
SELECT
    id AS account_id,
    collateral_volume::NUMERIC AS volume,
    num_trades::BIGINT AS trade_count,
    RANK() OVER (ORDER BY collateral_volume::NUMERIC DESC) AS volume_rank,
    NTILE(100) OVER (ORDER BY collateral_volume::NUMERIC DESC) AS volume_percentile
FROM accounts
WHERE collateral_volume IS NOT NULL AND collateral_volume != '' AND collateral_volume != '0';

CREATE UNIQUE INDEX IF NOT EXISTS idx_whale_rankings_id ON whale_rankings (account_id);

-- 4. Whale Flow (Recent large position changes)
-- Converted to Standard View for Real-time Access
CREATE OR REPLACE VIEW whale_flow_24h AS
SELECT
    account_id,
    market_id,
    SUM(delta_value) AS net_flow_usd,
    SUM(delta_quantity) AS net_flow_qty,
    COUNT(*) AS activity_count
FROM position_snapshots
WHERE snapshot_time > NOW() - INTERVAL '24 hours'
  AND ABS(delta_value) > 1000 -- Only significant moves
GROUP BY account_id, market_id;

-- Sync state for incremental syncing
CREATE TABLE IF NOT EXISTS sync_state (
    sync_type TEXT PRIMARY KEY,      -- e.g. 'accounts', 'conditions', 'events'
    last_cursor TEXT,                -- Last ID/offset synced
    last_sync_at TIMESTAMPTZ,
    total_items INT DEFAULT 0,
    status TEXT DEFAULT 'idle'       -- 'running', 'completed', 'failed'
);

-- 4. Top Sync Tables (Users & Holdings)

CREATE TABLE IF NOT EXISTS plymkt_users (
    proxy_wallet TEXT PRIMARY KEY,
    username TEXT,
    name TEXT,
    bio TEXT,
    profile_image TEXT,
    x_username TEXT,
    verified_badge BOOLEAN,
    -- Metrics (Latest snapshot from leaderboard/holders)
    vol NUMERIC,
    pnl NUMERIC,
    rank INT, -- Latest observed rank (context dependent, optional)
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS plymkt_holders (
    token_id TEXT,
    proxy_wallet TEXT REFERENCES plymkt_users(proxy_wallet),
    amount NUMERIC,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (token_id, proxy_wallet)
);
CREATE INDEX IF NOT EXISTS idx_plymkt_holders_token ON plymkt_holders (token_id);
CREATE INDEX IF NOT EXISTS idx_plymkt_holders_wallet ON plymkt_holders (proxy_wallet);

-- Orderbook Snapshots Hypertable
-- Stores computed orderbook depth metrics per token per snapshot time

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    time            TIMESTAMPTZ      NOT NULL,
    market_id       TEXT             NOT NULL,
    token_id        TEXT             NOT NULL,          -- YES or NO token address
    best_bid        DOUBLE PRECISION,
    best_ask        DOUBLE PRECISION,
    mid_price       DOUBLE PRECISION GENERATED ALWAYS AS ((best_bid + best_ask) / 2) STORED,
    spread          DOUBLE PRECISION GENERATED ALWAYS AS (best_ask - best_bid) STORED,
    imbalance       DOUBLE PRECISION,
    total_bid_depth DOUBLE PRECISION,
    total_ask_depth DOUBLE PRECISION,
    depth_json      JSONB,                              -- full bids + asks array
    raw_response_json JSONB                             -- entire response for debug
);

-- Convert to Hypertable (1 hour chunks)
SELECT create_hypertable('orderbook_snapshots', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Unique constraint for upserts
ALTER TABLE orderbook_snapshots
ADD CONSTRAINT orderbook_snapshots_pk_unique UNIQUE (time, market_id, token_id);

-- Indices
CREATE INDEX IF NOT EXISTS idx_ob_snap_market_time ON orderbook_snapshots (market_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_ob_snap_token_time ON orderbook_snapshots (token_id, time DESC);

-- Retention policy: 30 days
SELECT add_retention_policy('orderbook_snapshots', INTERVAL '30 days', if_not_exists => TRUE);

-- top, hottest markets
CREATE TABLE IF NOT EXISTS hot_markets_vol (
    time            TIMESTAMPTZ     NOT NULL,
    market_id       TEXT            NOT NULL,
    question        TEXT,
    volume_24hr     DOUBLE PRECISION,
    volume_total    DOUBLE PRECISION,
    liquidity_clob  DOUBLE PRECISION,
    liquidity_fallback DOUBLE PRECISION,
    spread          DOUBLE PRECISION,
    price_change_1d DOUBLE PRECISION,
    score           DOUBLE PRECISION,
    active          BOOLEAN,
    closed          BOOLEAN,
    rank            INTEGER,
    category        TEXT NOT NULL DEFAULT 'global',
    end_date        TIMESTAMPTZ,
    start_date      TIMESTAMPTZ,
    outcome_prices  TEXT[]
);

-- 2. Convert to Hypertable
-- Partition by time (daily chunks)
SELECT create_hypertable('hot_markets_vol', 'time', 
    chunk_time_interval => INTERVAL '1 day', 
    if_not_exists => TRUE
);

-- 3. Create Constraints & Indices
-- Unique constraint required for UPSERT (ON CONFLICT) operations
-- Must include the time column because it's a hypertable
ALTER TABLE hot_markets_vol 
ADD CONSTRAINT hot_markets_vol_pk_unique UNIQUE (time, market_id, category);

-- Optimized lookup indices
CREATE INDEX IF NOT EXISTS idx_hot_market_time ON hot_markets_vol (market_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_hot_vol_24hr ON hot_markets_vol (volume_24hr DESC, time DESC);
CREATE INDEX IF NOT EXISTS idx_hot_score ON hot_markets_vol (score DESC, time DESC);
CREATE INDEX IF NOT EXISTS idx_hot_category ON hot_markets_vol (category, time DESC);

CREATE TABLE IF NOT EXISTS oi_history (
    time         TIMESTAMPTZ      NOT NULL,
    condition_id TEXT             NOT NULL,
    oi_value     DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('oi_history', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

ALTER TABLE oi_history
ADD CONSTRAINT oi_history_pk UNIQUE (time, condition_id);

CREATE INDEX IF NOT EXISTS idx_oi_condition_time
    ON oi_history (condition_id, time DESC);

-- Continuous aggregate for hourly OI deltas
CREATE MATERIALIZED VIEW IF NOT EXISTS oi_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    condition_id,
    last(oi_value, time) AS oi_close,
    first(oi_value, time) AS oi_open,
    max(oi_value) AS oi_high,
    min(oi_value) AS oi_low
FROM oi_history
GROUP BY bucket, condition_id;

-- Hot events (Gamma-ranked events snapshot hypertable)
CREATE TABLE IF NOT EXISTS hot_events (
    time TIMESTAMPTZ NOT NULL,
    id TEXT NOT NULL,
    ticker TEXT,
    slug TEXT,
    title TEXT,
    subtitle TEXT,
    description TEXT,
    resolution_source TEXT,
    start_date TIMESTAMPTZ,
    creation_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    image TEXT,
    icon TEXT,
    active BOOLEAN,
    closed BOOLEAN,
    archived BOOLEAN,
    new BOOLEAN,
    featured BOOLEAN,
    restricted BOOLEAN,
    liquidity NUMERIC,
    volume NUMERIC,
    open_interest NUMERIC,
    sort_by TEXT,
    category TEXT,
    subcategory TEXT,
    is_template BOOLEAN,
    template_variables TEXT,
    published_at TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    comments_enabled BOOLEAN,
    competitive NUMERIC,
    volume_24hr NUMERIC,
    volume_1wk NUMERIC,
    volume_1mo NUMERIC,
    volume_1yr NUMERIC,
    featured_image TEXT,
    disqus_thread TEXT,
    parent_event TEXT,
    enable_order_book BOOLEAN,
    liquidity_amm NUMERIC,
    liquidity_clob NUMERIC,
    neg_risk BOOLEAN,
    neg_risk_market_id TEXT,
    neg_risk_fee_bips INTEGER,
    comment_count INTEGER,
    cyom BOOLEAN,
    tags JSONB,
    sub_events TEXT[],
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('hot_events', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_hot_events_category ON hot_events (category, time DESC);
CREATE INDEX IF NOT EXISTS idx_hot_events_volume_24hr ON hot_events (volume_24hr DESC, time DESC);

-- Trades from Polymarket data API
CREATE TABLE IF NOT EXISTS trades (
    transaction_hash TEXT PRIMARY KEY,
    proxy_wallet     TEXT,
    side             TEXT,
    asset            TEXT,
    condition_id     TEXT,
    size             INTEGER,
    price            INTEGER,
    timestamp        INTEGER,
    title            TEXT,
    slug             TEXT,
    icon             TEXT,
    event_slug       TEXT,
    outcome          TEXT,
    outcome_index    INTEGER,
    name             TEXT,
    pseudonym        TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);