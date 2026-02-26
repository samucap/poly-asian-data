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
