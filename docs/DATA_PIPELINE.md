# Data Pipeline Runbook

## Quick Commands

```bash
make ingest-primary
make derive-topics
make claim-candidates
make derive-entities
make claim-quality
make load-db
make daily-report
make coverage-gaps
make daily-pipeline
```

## Pipelines

1. `scripts/ingest_primary_authority_docs.py`
   - Pulls latest docs from CourtListener search, CourtListener RECAP (PACER filings), House Judiciary, GovTrack, GovInfo, and DOJ OPA.
   - Applies phrase filtering (`Jeffrey Epstein`, `Ghislaine Maxwell`, `Epstein files`) to RECAP/transcript results to reduce unrelated records.
   - Adds provenance columns (`source_tier`, `capture_method`, `content_checksum`) at ingest time.
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

5. `scripts/derive_entity_mentions.py`
   - Resolves canonical entities + aliases (with fuzzy matching) and emits context-typed mentions.
   - Writes outputs to `derived/entities/`.

6. `scripts/assess_claim_context_quality.py`
   - Applies quality rules (`name_only_implication_risk`, `no_direct_context`, etc.) to claims.
   - Writes `claim_quality_flags_latest.tsv` and summary to `derived/claims/`.

7. `scripts/generate_daily_change_report.py`
   - Compares latest two primary-doc snapshots.
   - Snapshots claim registry into `derived/claims/history/`.
   - Computes claim-status diffs from latest two claim snapshots.
    - Summarizes latest claim-quality flag severities.
   - Writes reports to `derived/reports/`.

8. `scripts/generate_coverage_gap_dashboard.py`
   - Compares expected DOJ data-set range vs detected ingested set links.
   - Flags broken endpoints, stale inputs, and missing expected source systems.
   - Writes dashboard + metrics to `derived/reports/`.

9. `scripts/run_daily_pipeline.sh`
   - End-to-end wrapper: ingest, derive topics, generate claim candidates, derive entities, assess claim quality, load DB, generate diff/coverage reports.

## Output Map

- `derived/primary_docs/primary_documents_latest.tsv`
- `derived/topics/primary_doc_topic_index_latest.tsv`
- `derived/claims/claim_candidates_latest.tsv`
- `derived/entities/entity_aliases_resolved_latest.tsv`
- `derived/entities/entity_mentions_latest.tsv`
- `derived/claims/claim_quality_flags_latest.tsv`
- `derived/database/epstein_research.sqlite`
- `derived/reports/daily_change_report_latest.md`
- `derived/reports/daily_primary_doc_diff_latest.tsv`
- `derived/reports/daily_claim_status_changes_latest.tsv`
- `derived/reports/coverage_gap_dashboard_latest.md`
- `derived/reports/coverage_gap_metrics_latest.tsv`

## Suggested Daily Routine

1. Run `make daily-pipeline`.
2. Review `derived/reports/daily_change_report_latest.md`.
3. Review `derived/reports/coverage_gap_dashboard_latest.md` for missing datasets/sources.
4. Promote verified new facts into `derived/claims/claim_registry_latest.tsv` and evidence links.
5. Address high-severity rows in `derived/claims/claim_quality_flags_latest.tsv`.
6. Commit both source changes and generated artifacts in one commit for traceability.
