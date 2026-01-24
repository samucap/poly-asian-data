-- Database: plymkt
-- Migration: 001_initial
-- Tables for Polymarket sports data

-- Sport table
CREATE TABLE IF NOT EXISTS sport (
    sport VARCHAR(255) PRIMARY KEY,
    image TEXT,
    resolution TEXT,
    ordering TEXT,
    tags TEXT,
    series TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Team table
CREATE TABLE IF NOT EXISTS team (
    id INTEGER PRIMARY KEY,
    league VARCHAR(100),
    name VARCHAR(255),
    record VARCHAR(50),
    logo TEXT,
    abbreviation VARCHAR(20),
    alias VARCHAR(255),
    provider_id INTEGER,
    color VARCHAR(20),
    created_at_ply TIMESTAMPTZ,
    updated_at_ply TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tag (
    id VARCHAR(255) PRIMARY KEY,
    label VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    force_show BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    created_at_ply TIMESTAMPTZ,
    updated_at_ply TIMESTAMPTZ,
    force_hide BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_team_league ON team(league);
CREATE INDEX IF NOT EXISTS idx_team_name ON team(name);
CREATE INDEX IF NOT EXISTS idx_tag_id ON tag(id);

-- TODO: Add tables for market, event, category, tag, collection, series
-- with appropriate FK relationships once reviewed
