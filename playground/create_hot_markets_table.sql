-- 1. Create table structure
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
    clob_token_ids  JSONB,
    active          BOOLEAN,
    closed          BOOLEAN,
    last_fetched    TIMESTAMPTZ     DEFAULT NOW(),
    rank_in_batch   INTEGER,        -- Deprecated in favor of rank? Or keep as alias?
    rank            INTEGER,        -- Rank within the specific category list
    category        TEXT            NOT NULL DEFAULT 'global'
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
