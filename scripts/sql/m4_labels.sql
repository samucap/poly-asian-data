-- M4 label storage (PIT). Create when implementing edge-eval writers.
-- Labels must join features only on as_of <= label decision time (no lookahead).

CREATE TABLE IF NOT EXISTS label_rows (
    decision_time   TIMESTAMPTZ NOT NULL,
    horizon         TEXT        NOT NULL,  -- 5m | 1h | 1d
    condition_id    TEXT        NOT NULL,
    selection_set   TEXT        NOT NULL DEFAULT 'board_at_t',
    run_id          TEXT,
    -- Outcome after costs (fill model applied offline)
    hit             BOOLEAN,
    after_cost_return_bps DOUBLE PRECISION,
    -- Strata snapshots at decision time (denormalized for fast eval)
    category        TEXT,
    neg_risk        BOOLEAN,
    fv_source       TEXT,
    ttr_bucket      TEXT,
    mid_at_t        DOUBLE PRECISION,
    edge_bps_at_t   DOUBLE PRECISION,  -- diagnostic only; not a model input
    PRIMARY KEY (decision_time, horizon, condition_id, selection_set)
);

CREATE INDEX IF NOT EXISTS idx_label_rows_horizon_time
    ON label_rows (horizon, decision_time DESC);

CREATE INDEX IF NOT EXISTS idx_label_rows_strata
    ON label_rows (category, neg_risk, fv_source);

COMMENT ON TABLE label_rows IS 'PIT labels for M4 edge-eval; never join future feature rows into decision features';
COMMENT ON COLUMN label_rows.edge_bps_at_t IS 'Diagnostic snapshot only — forbidden as training/eval model input feature';
