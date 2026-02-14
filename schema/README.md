# Normalized Schema (Phase 1)

This folder defines the baseline relational model for moving this repository from file-centric notes to a queryable evidence database.

## Files

- `epstein_research_schema.sql`: SQLite-compatible DDL.

## Core Model

- `documents`: canonical source records (court filings, hearings, releases, statutes).
- `entities` + `entity_aliases`: people/orgs/agencies and name resolution.
- `events`: dated actions tied to source docs.
- `claims`: atomic claims with status and confidence.
- `claim_candidates`: pending-review claims auto-derived from primary docs.
- `claim_evidence_links`: evidence graph linking each claim to specific docs.
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
