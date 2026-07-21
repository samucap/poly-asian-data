-- M3 feature store DDL (Timescale-aware). Safe to re-run.
-- Prefer application EnsureM3FeatureTables for runtime; this is ops/docs.

CREATE TABLE IF NOT EXISTS oi_history (
    time          TIMESTAMPTZ      NOT NULL,
    condition_id  TEXT             NOT NULL,
    oi_value      DOUBLE PRECISION NOT NULL,
    source        TEXT DEFAULT 'data-api'
);
-- Pre-M3 tables may lack source; CREATE IF NOT EXISTS does not add columns.
ALTER TABLE oi_history ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'data-api';

SELECT create_hypertable('oi_history', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_oi_history_cid_time ON oi_history (condition_id, time DESC);
SELECT add_retention_policy('oi_history', INTERVAL '90 days', if_not_exists => TRUE);

-- edge_board M3 columns
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS cost_bps DOUBLE PRECISION;
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS capacity_usd DOUBLE PRECISION;
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS urgency DOUBLE PRECISION;
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS key_features JSONB;
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS risk_flags TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS strategy_tags TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS features_asof TIMESTAMPTZ;
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS fair_value DOUBLE PRECISION;
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS model_edge_bps DOUBLE PRECISION;
ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS fv_source TEXT;

-- Optional continuous aggregate on orderbook_snapshots (create only after writers live).
-- Uncomment when orderbook_snapshots has data:
/*
CREATE MATERIALIZED VIEW IF NOT EXISTS feature_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    token_id,
    last(best_bid, time) AS best_bid,
    last(best_ask, time) AS best_ask,
    avg(imbalance) AS imbalance,
    avg(total_bid_depth) AS bid_depth,
    avg(total_ask_depth) AS ask_depth
FROM orderbook_snapshots
GROUP BY bucket, token_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('feature_1m',
    start_offset => INTERVAL '2 hours',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);
*/
