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
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Deferrable Constraint to allow circular references during transaction
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
    id TEXT PRIMARY KEY, -- transaction hash
    price TEXT,
    side TEXT,
    size TEXT,
    maker_id TEXT,
    taker_id TEXT,
    market_id TEXT,
    timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    fee TEXT,
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
    spread NUMERIC,
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
    raw_json JSONB
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
