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
  source_tier TEXT NOT NULL DEFAULT 'tier2', -- tier1_primary, tier2_secondary, tier3_aggregate
  capture_method TEXT NOT NULL DEFAULT 'manual', -- api, sitemap, html_scrape, manual
  content_checksum TEXT, -- sha1 of canonical source payload metadata
  document_type TEXT NOT NULL,
  jurisdiction TEXT,
  title TEXT NOT NULL,
  doc_date TEXT,
  url TEXT NOT NULL UNIQUE,
  citation TEXT,
  status TEXT,
  extracted_from TEXT,
  first_seen_run_id TEXT,
  last_seen_run_id TEXT,
  first_seen_at_utc TEXT NOT NULL DEFAULT '',
  last_seen_at_utc TEXT NOT NULL DEFAULT '',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(first_seen_run_id) REFERENCES ingest_runs(run_id),
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
  status TEXT NOT NULL, -- verified_primary, verified_secondary, alleged, disputed, retracted, pending_review
  confidence REAL CHECK(confidence >= 0.0 AND confidence <= 1.0),
  notes TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(asserted_by_entity_id) REFERENCES entities(entity_id)
);

CREATE INDEX IF NOT EXISTS idx_claims_status ON claims(status);

CREATE TABLE IF NOT EXISTS evidence_spans (
  span_id TEXT PRIMARY KEY,
  doc_id TEXT NOT NULL,
  locator_type TEXT NOT NULL, -- page, section, line, timestamp, unknown
  locator_value TEXT NOT NULL, -- e.g. p.12, line 44, timestamp 00:03:15
  snippet_text TEXT,
  snippet_hash TEXT, -- sha1(snippet_text)
  source_url TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_evidence_spans_doc ON evidence_spans(doc_id);
CREATE INDEX IF NOT EXISTS idx_evidence_spans_hash ON evidence_spans(snippet_hash);

CREATE TABLE IF NOT EXISTS claim_evidence_links (
  claim_evidence_id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  evidence_type TEXT NOT NULL, -- primary, secondary, transcript, filing, release
  evidence_strength TEXT NOT NULL, -- direct, supporting, contextual, contradictory
  evidence_locator TEXT, -- human readable locator, e.g. "Data Set 12, page 8"
  quote_excerpt TEXT,
  snippet_hash TEXT, -- sha1 of quote_excerpt or source snippet
  span_id TEXT,
  evidence_url TEXT,
  provenance_note TEXT,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(claim_id) REFERENCES claims(claim_id) ON DELETE CASCADE,
  FOREIGN KEY(doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE,
  FOREIGN KEY(span_id) REFERENCES evidence_spans(span_id)
);

CREATE INDEX IF NOT EXISTS idx_claim_evidence_claim ON claim_evidence_links(claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_doc ON claim_evidence_links(doc_id);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_span ON claim_evidence_links(span_id);

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

CREATE TABLE IF NOT EXISTS entity_mentions (
  mention_id TEXT PRIMARY KEY,
  doc_id TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  mention_text TEXT NOT NULL,
  normalized_mention_text TEXT NOT NULL,
  context_type TEXT NOT NULL, -- news_clipping, email_sender, email_body, flight_log, legal_filing, allegation, general_reference
  context_snippet TEXT,
  mention_count INTEGER NOT NULL DEFAULT 1,
  confidence REAL CHECK(confidence >= 0.0 AND confidence <= 1.0),
  source_span_id TEXT,
  source_url TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE,
  FOREIGN KEY(entity_id) REFERENCES entities(entity_id) ON DELETE CASCADE,
  FOREIGN KEY(source_span_id) REFERENCES evidence_spans(span_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_mentions_doc ON entity_mentions(doc_id);
CREATE INDEX IF NOT EXISTS idx_entity_mentions_entity ON entity_mentions(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_mentions_context_type ON entity_mentions(context_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_mentions_unique
  ON entity_mentions(doc_id, entity_id, normalized_mention_text, context_type);

CREATE TABLE IF NOT EXISTS claim_quality_flags (
  flag_id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL,
  rule_id TEXT NOT NULL, -- no_direct_context, name_only_implication_risk, no_primary_evidence, unsupported_criminal_inference
  severity TEXT NOT NULL, -- info, warn, high
  flag_status TEXT NOT NULL, -- open, resolved
  message TEXT NOT NULL,
  related_doc_id TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(claim_id) REFERENCES claims(claim_id) ON DELETE CASCADE,
  FOREIGN KEY(related_doc_id) REFERENCES documents(doc_id)
);

CREATE INDEX IF NOT EXISTS idx_claim_quality_claim ON claim_quality_flags(claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_quality_severity ON claim_quality_flags(severity);

CREATE TABLE IF NOT EXISTS claim_review_queue (
  queue_id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL,
  priority TEXT NOT NULL, -- p1, p2, p3
  claim_status TEXT NOT NULL,
  triage_status TEXT NOT NULL, -- open, in_review, resolved, deferred
  flag_count INTEGER NOT NULL DEFAULT 0,
  high_flag_count INTEGER NOT NULL DEFAULT 0,
  warn_flag_count INTEGER NOT NULL DEFAULT 0,
  rule_ids TEXT NOT NULL,
  evidence_gap TEXT NOT NULL,
  recommended_action TEXT NOT NULL,
  related_doc_ids TEXT,
  related_source_urls TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY(claim_id) REFERENCES claims(claim_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_claim_review_claim ON claim_review_queue(claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_review_priority ON claim_review_queue(priority);
CREATE INDEX IF NOT EXISTS idx_claim_review_status ON claim_review_queue(triage_status);

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
