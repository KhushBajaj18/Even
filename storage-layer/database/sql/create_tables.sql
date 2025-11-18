CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS feedback_events (
  event_id UUID PRIMARY KEY,
  customer_id TEXT,
  source TEXT,
  source_id TEXT,
  ts TIMESTAMPTZ,
  raw_text TEXT,
  redacted_text TEXT,
  raw_blob_url TEXT,
  metadata JSONB,
  canonical_json JSONB,
  language TEXT,
  has_pii BOOLEAN,
  quality_score REAL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_feedback_customer ON feedback_events(customer_id);
CREATE INDEX IF NOT EXISTS idx_feedback_source_ts ON feedback_events(source, ts);
CREATE INDEX IF NOT EXISTS idx_metadata_gin ON feedback_events USING GIN (metadata);
