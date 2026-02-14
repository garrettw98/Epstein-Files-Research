-- Normalized schema for Epstein research database.
-- Target: SQLite 3.x

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id TEXT PRIMARY KEY,
  ingested_at_utc TEXT NOT NULL,
  pipeline_name TEXT NOT NULL,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS documents (
  doc_id TEXT PRIMARY KEY,
  source_system TEXT NOT NULL,
  document_type TEXT NOT NULL,
  jurisdiction TEXT,
  title TEXT NOT NULL,
  doc_date TEXT,
  url TEXT NOT NULL UNIQUE,
  citation TEXT,
  status TEXT,
  extracted_from TEXT,
  last_seen_run_id TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(last_seen_run_id) REFERENCES ingest_runs(run_id)
);

CREATE TABLE IF NOT EXISTS entities (
  entity_id TEXT PRIMARY KEY,
  entity_type TEXT NOT NULL, -- person, organization, agency, court, committee, place
  canonical_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  notes TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entities_name_type
  ON entities(normalized_name, entity_type);

CREATE TABLE IF NOT EXISTS entity_aliases (
  alias_id TEXT PRIMARY KEY,
  entity_id TEXT NOT NULL,
  alias_name TEXT NOT NULL,
  normalized_alias_name TEXT NOT NULL,
  alias_type TEXT NOT NULL, -- legal_name, short_name, honorific, transliteration, nickname
  source_doc_id TEXT,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(entity_id) REFERENCES entities(entity_id) ON DELETE CASCADE,
  FOREIGN KEY(source_doc_id) REFERENCES documents(doc_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_alias_unique
  ON entity_aliases(entity_id, normalized_alias_name, alias_type);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL, -- hearing, release, filing, vote, indictment, verdict
  title TEXT NOT NULL,
  event_date TEXT,
  event_datetime_utc TEXT,
  jurisdiction TEXT,
  summary TEXT,
  source_doc_id TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(source_doc_id) REFERENCES documents(doc_id)
);

CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date);

CREATE TABLE IF NOT EXISTS claims (
  claim_id TEXT PRIMARY KEY,
  claim_text TEXT NOT NULL,
  claim_type TEXT NOT NULL, -- factual, procedural, legal, allegation, timeline
  asserted_by_entity_id TEXT,
  first_seen_date TEXT,
  status TEXT NOT NULL, -- verified, disputed, unverified, retracted, pending_review
  confidence REAL CHECK(confidence >= 0.0 AND confidence <= 1.0),
  notes TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(asserted_by_entity_id) REFERENCES entities(entity_id)
);

CREATE INDEX IF NOT EXISTS idx_claims_status ON claims(status);

CREATE TABLE IF NOT EXISTS claim_evidence_links (
  claim_evidence_id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  evidence_type TEXT NOT NULL, -- primary, secondary, transcript, filing, release
  evidence_strength TEXT NOT NULL, -- direct, supporting, contextual, contradictory
  quote_excerpt TEXT,
  evidence_url TEXT,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(claim_id) REFERENCES claims(claim_id) ON DELETE CASCADE,
  FOREIGN KEY(doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_claim_evidence_claim ON claim_evidence_links(claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_doc ON claim_evidence_links(doc_id);

CREATE TABLE IF NOT EXISTS claim_contradictions (
  contradiction_id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL,
  contradictory_doc_id TEXT NOT NULL,
  notes TEXT,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(claim_id) REFERENCES claims(claim_id) ON DELETE CASCADE,
  FOREIGN KEY(contradictory_doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_claim_contradictions_claim ON claim_contradictions(claim_id);

CREATE TABLE IF NOT EXISTS claim_candidates (
  candidate_id TEXT PRIMARY KEY,
  claim_text TEXT NOT NULL,
  claim_type TEXT NOT NULL,
  asserted_by TEXT,
  first_seen_date TEXT,
  proposed_status TEXT NOT NULL, -- pending_review, promoted, discarded
  confidence REAL CHECK(confidence >= 0.0 AND confidence <= 1.0),
  topic_id TEXT,
  evidence_doc_id TEXT,
  evidence_url TEXT,
  rationale TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(evidence_doc_id) REFERENCES documents(doc_id)
);

CREATE INDEX IF NOT EXISTS idx_claim_candidates_status ON claim_candidates(proposed_status);
CREATE INDEX IF NOT EXISTS idx_claim_candidates_topic ON claim_candidates(topic_id);
