# Data Pipeline Runbook

## Quick Commands

```bash
make ingest-primary
make derive-topics
make claim-candidates
make load-db
make daily-report
make daily-pipeline
```

## Pipelines

1. `scripts/ingest_primary_authority_docs.py`
   - Pulls latest docs from CourtListener search, CourtListener RECAP (PACER filings), House Judiciary, GovTrack, GovInfo, and DOJ OPA.
   - Applies phrase filtering (`Jeffrey Epstein`, `Ghislaine Maxwell`, `Epstein files`) to RECAP/transcript results to reduce unrelated records.
   - Writes snapshots to `raw/primary_docs/` and indexes to `derived/primary_docs/`.

2. `scripts/load_epstein_sqlite.py`
   - Applies `schema/epstein_research_schema.sql` and loads primary docs + claim/evidence/candidate TSVs into `derived/database/epstein_research.sqlite`.
   - By default, prunes managed-source docs that are no longer present in `primary_documents_latest.tsv` (use `--no-prune-missing-docs` to keep historical rows).

3. `scripts/derive_primary_doc_topics.py`
   - Tags each primary doc with one or more taxonomy topics.
   - Writes topic index and catalog outputs to `derived/topics/`.

4. `scripts/generate_claim_candidates.py`
   - Generates pending-review claims from primary docs and topic hints.
   - Writes claim candidate backlog outputs to `derived/claims/`.

5. `scripts/generate_daily_change_report.py`
   - Compares latest two primary-doc snapshots.
   - Snapshots claim registry into `derived/claims/history/`.
   - Computes claim-status diffs from latest two claim snapshots.
   - Writes reports to `derived/reports/`.

6. `scripts/run_daily_pipeline.sh`
   - End-to-end wrapper: ingest, derive topics, generate claim candidates, load DB, generate diff reports.

## Output Map

- `derived/primary_docs/primary_documents_latest.tsv`
- `derived/topics/primary_doc_topic_index_latest.tsv`
- `derived/claims/claim_candidates_latest.tsv`
- `derived/database/epstein_research.sqlite`
- `derived/reports/daily_change_report_latest.md`
- `derived/reports/daily_primary_doc_diff_latest.tsv`
- `derived/reports/daily_claim_status_changes_latest.tsv`

## Suggested Daily Routine

1. Run `make daily-pipeline`.
2. Review `derived/reports/daily_change_report_latest.md`.
3. Promote verified new facts into `derived/claims/claim_registry_latest.tsv` and evidence links.
4. Commit both source changes and generated artifacts in one commit for traceability.
