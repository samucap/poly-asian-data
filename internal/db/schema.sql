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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tags (
    id TEXT PRIMARY KEY,
    label TEXT,
    slug TEXT,
    force_show BOOLEAN,
    force_hide BOOLEAN,
    sport_id UUID REFERENCES sports(id),
    parent_tag_id TEXT REFERENCES tags(id) DEFERRABLE INITIALLY DEFERRED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
