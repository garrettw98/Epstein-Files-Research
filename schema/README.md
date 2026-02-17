# Normalized Schema (Phase 1)

This folder defines the baseline relational model for moving this repository from file-centric notes to a queryable evidence database.

## Files

- `epstein_research_schema.sql`: SQLite-compatible DDL.

## Core Model

- `documents`: canonical source records with provenance (`source_tier`, `capture_method`, checksums, first/last seen).
- `entities` + `entity_aliases`: people/orgs/agencies and name resolution.
- `entity_mentions`: context-typed mentions (`news_clipping`, `email_body`, `legal_filing`, etc.) per document.
- `events`: dated actions tied to source docs.
- `claims`: atomic claims with status and confidence.
- `claim_candidates`: pending-review claims auto-derived from primary docs.
- `evidence_spans`: normalized page/section/line/timestamp anchors with snippet hashes.
- `claim_evidence_links`: evidence graph linking each claim to specific docs and span-level citations.
- `claim_quality_flags`: rule-engine flags for weak inference patterns (e.g., name-only implication risk).
- `claim_review_queue`: prioritized triage queue generated from open claim quality flags.
- `claim_contradictions`: explicit contradiction tracking.
- `ingest_runs`: provenance for import/update pipelines.

## Quick Start

```bash
sqlite3 derived/database/epstein_research.sqlite < schema/epstein_research_schema.sql
```

Then ingest source docs:

```bash
./scripts/ingest_primary_authority_docs.py
```

Derive topics and claim candidates:

```bash
./scripts/derive_primary_doc_topics.py
./scripts/generate_claim_candidates.py
```

Then load normalized tables:

```bash
./scripts/load_epstein_sqlite.py
```

Claim/evidence TSVs are sourced from `derived/claims/`.
Candidate claims are sourced from `derived/claims/claim_candidates_latest.tsv`.
