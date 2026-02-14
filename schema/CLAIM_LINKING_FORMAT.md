# Claim-Evidence Linking Format

Use this format when adding or updating claims so every assertion remains auditable.

## Claim Registry File

Path: `derived/claims/claim_registry_latest.tsv`

Required columns:

- `claim_id`: stable ID (`claim-<slug-or-hash>`).
- `claim_text`: one atomic statement.
- `claim_type`: `factual|procedural|legal|allegation|timeline`.
- `asserted_by`: person/org/agency string (or `system`).
- `first_seen_date`: `YYYY-MM-DD` when claim first appears in known record.
- `status`: `verified|disputed|unverified|retracted|pending_review`.
- `confidence`: `0.0` to `1.0`.
- `notes`: brief qualification or scope note.

## Claim Evidence Links File

Path: `derived/claims/claim_evidence_links_latest.tsv`

Required columns:

- `claim_id`: foreign key to claim registry.
- `doc_id`: canonical document ID from primary docs ingest (preferred) or stable pseudo-ID.
- `evidence_type`: `primary|secondary|transcript|filing|release`.
- `evidence_strength`: `direct|supporting|contextual|contradictory`.
- `evidence_url`: URL to the referenced source.
- `quote_excerpt`: concise excerpt or summary anchor.

## Rules

- Keep one claim per row. Split compound claims.
- Prefer primary docs over secondary reports whenever available.
- If status changes, update row in place and retain a clear note.
- If evidence contradicts a verified claim, downgrade status until resolved.
