-- Create tags table for storing Polymarket category tags
CREATE TABLE IF NOT EXISTS tags (
    id TEXT PRIMARY KEY,
    label TEXT,
    slug TEXT,
    force_show BOOLEAN,
    force_hide BOOLEAN,
    sport_id UUID,
    parent_tag_id TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Add self-referential foreign key (deferrable for circular inserts)
ALTER TABLE tags DROP CONSTRAINT IF EXISTS fk_tags_parent;
ALTER TABLE tags ADD CONSTRAINT fk_tags_parent 
    FOREIGN KEY (parent_tag_id) REFERENCES tags(id) 
    DEFERRABLE INITIALLY DEFERRED;

-- Trigger for auto-updating updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_tags_updated_at ON tags;
CREATE TRIGGER update_tags_updated_at BEFORE UPDATE ON tags FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
