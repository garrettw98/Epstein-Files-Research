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
- `status`: `verified_primary|verified_secondary|alleged|disputed|retracted|pending_review`.
- `confidence`: `0.0` to `1.0`.
- `name_context_class`: `direct_contact|administrative_mention|media_reference|unverified_allegation|unknown`.
- `notes`: brief qualification or scope note.

## Claim Evidence Links File

Path: `derived/claims/claim_evidence_links_latest.tsv`

Required columns:

- `claim_id`: foreign key to claim registry.
- `doc_id`: canonical document ID from primary docs ingest (preferred) or stable pseudo-ID.
- `evidence_type`: `primary|secondary|transcript|filing|release`.
- `evidence_strength`: `direct|supporting|contextual|contradictory`.
- `evidence_locator`: optional page/section/line locator when known.
- `evidence_url`: URL to the referenced source.
- `quote_excerpt`: concise excerpt or summary anchor.
- `snippet_hash`: optional SHA1 for exact snippet traceability.
- `span_id`: optional FK into `evidence_spans` for normalized span-level citation reuse.
- `provenance_note`: optional note about extraction method (`api`, `html_scrape`, `manual_review`).

## Rules

- Keep one claim per row. Split compound claims.
- Prefer primary docs over secondary reports whenever available.
- If status changes, update row in place and retain a clear note.
- If evidence contradicts a verified claim, downgrade status until resolved.
- A name-only mention does not imply misconduct; keep such claims at `alleged` or `verified_secondary` unless direct context exists.
- For high-risk claims, attach a locator and snippet hash so evidence can be re-audited quickly.
