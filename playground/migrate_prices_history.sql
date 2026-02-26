-- Migration: fix prices_history schema to match API data types
-- The old schema had 'timestamp' as TIMESTAMPTZ; the API returns Unix epoch integers.
-- This migration recreates the table with the correct types.
-- WARNING: This will drop existing data. Run only if you're OK with re-fetching.

-- Step 1: Drop the old table
DROP TABLE IF EXISTS prices_history;

-- Step 2: Recreate with correct column types
CREATE TABLE prices_history (
    token_id     TEXT             NOT NULL,
    timestamp    BIGINT           NOT NULL,
    price        DOUBLE PRECISION NOT NULL,
    market_id    TEXT,
    fidelity_min INTEGER,
    updated_at   BIGINT,
    PRIMARY KEY (token_id, timestamp)
);
