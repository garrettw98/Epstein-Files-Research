# Repository Organization

This guide defines where content belongs so additions stay readable and auditable.

## Directory Contract

- `profiles/` - Person/entity profiles.
- `topics/` - Thematic explainers and deep dives.
- `evidence/` - Source-backed briefs and dossiers.
- `timeline/` - Chronological products.
- `raw/` - Immutable source snapshots from ingest scripts.
- `derived/` - Machine-readable normalized outputs generated from `raw/`.
- `schema/` - Relational schema and claim-linking standards.
- `scripts/` - Ingestion, normalization, and reporting automation.
- `analysis/` - Synthetic analysis products based on sourced data.
- `updates/` - Live update event-line inputs.

## File Naming Rules

- Use `Title_Case.md` for narrative markdown in `profiles/`, `topics/`, and `evidence/`.
- Use `snake_case` for script names and generated TSV/JSON files.
- Timestamped generated files use `YYYYMMDDTHHMMSSZ`.
- Keep a `*_latest.*` companion file for each generated artifact.

## Editing Rules

- Do not manually edit files inside `raw/`.
- Treat `derived/` as generated artifacts; update by rerunning scripts.
- Every non-trivial claim should have a row in `derived/claims/claim_registry_latest.tsv` and at least one row in `derived/claims/claim_evidence_links_latest.tsv`.
- If a claim status changes, update claim notes with why and source basis.

## Readability Rules

- Keep evidence claims atomic (one claim per line/row).
- Prefer direct links to primary documents over secondary summaries.
- In long markdown files, include short section headers every 100-150 lines.
- Keep speculation isolated and explicitly labeled separately from verified findings.
