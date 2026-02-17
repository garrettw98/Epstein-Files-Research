# Data Pipeline Runbook

## Quick Commands

```bash
make ingest-library
make dataset-completeness
make ingest-primary
make derive-topics
make claim-candidates
make derive-entities
make claim-quality
make claim-triage
make load-db
make daily-report
make coverage-gaps
make last24h-brief
make daily-pipeline
```

## Pipelines

1. `scripts/ingest_primary_authority_docs.py`
   - Pulls latest docs from CourtListener search, CourtListener RECAP (PACER filings), House Judiciary, GovTrack, GovInfo, and DOJ OPA.
   - Applies phrase filtering (`Jeffrey Epstein`, `Ghislaine Maxwell`, `Epstein files`) to RECAP/transcript results to reduce unrelated records.
   - Adds provenance columns (`source_tier`, `capture_method`, `content_checksum`) at ingest time.
   - Writes snapshots to `raw/primary_docs/` and indexes to `derived/primary_docs/`.

2. `scripts/ingest_epstein_library.sh`
   - Pulls DOJ Epstein library root/disclosures pages into raw snapshots and a normalized link index.
   - Feeds data-set URL detection used by coverage checks.

3. `scripts/derive_doj_dataset_completeness.py`
   - Crawls each DOJ `data-set-<n>-files` listing page (including pagination).
   - Writes per-set file counts and file indexes to `derived/doj_epstein_library/`.

4. `scripts/load_epstein_sqlite.py`
   - Applies `schema/epstein_research_schema.sql` and loads primary docs + claim/evidence/candidate TSVs into `derived/database/epstein_research.sqlite`.
   - By default, prunes managed-source docs that are no longer present in `primary_documents_latest.tsv` (use `--no-prune-missing-docs` to keep historical rows).

5. `scripts/derive_primary_doc_topics.py`
   - Tags each primary doc with one or more taxonomy topics.
   - Writes topic index and catalog outputs to `derived/topics/`.

6. `scripts/generate_claim_candidates.py`
   - Generates pending-review claims from primary docs and topic hints.
   - Writes claim candidate backlog outputs to `derived/claims/`.

7. `scripts/derive_entity_mentions.py`
   - Resolves canonical entities + aliases (with fuzzy matching) and emits context-typed mentions.
   - Writes outputs to `derived/entities/`.

8. `scripts/assess_claim_context_quality.py`
   - Applies quality rules (`name_only_implication_risk`, `no_direct_context`, etc.) to claims.
   - Writes `claim_quality_flags_latest.tsv` and summary to `derived/claims/`.

9. `scripts/triage_claim_quality_flags.py`
   - Converts open claim quality flags into prioritized triage queue rows (`p1/p2/p3`).
   - Writes `claim_review_queue_latest.tsv` and summary to `derived/claims/`.

10. `scripts/generate_daily_change_report.py`
   - Compares latest two primary-doc snapshots.
   - Snapshots claim registry into `derived/claims/history/`.
   - Computes claim-status diffs from latest two claim snapshots.
   - Summarizes latest claim-quality flag severities.
   - Writes reports to `derived/reports/`.

11. `scripts/generate_coverage_gap_dashboard.py`
   - Compares expected DOJ data-set range vs detected ingested set links.
   - Adds per-data-set file-count checks and zero-file set warnings.
   - Flags broken endpoints, stale inputs, and missing expected source systems.
   - Writes dashboard + metrics to `derived/reports/`.

12. `scripts/update_last24h_brief.py`
   - Builds a rolling 24-hour change brief from latest ingest/report artifacts.
   - Auto-updates managed brief blocks in `README.md` and `timeline/Full_Timeline.md`.

13. `scripts/run_daily_pipeline.sh`
   - End-to-end wrapper: ingest library, derive dataset completeness, ingest primary docs, derive topics, generate claims/entities/quality/triage, load DB, generate reports, update 24-hour brief.

## Output Map

- `derived/doj_epstein_library/dataset_file_counts_latest.tsv`
- `derived/doj_epstein_library/dataset_file_index_latest.tsv`
- `derived/primary_docs/primary_documents_latest.tsv`
- `derived/topics/primary_doc_topic_index_latest.tsv`
- `derived/claims/claim_candidates_latest.tsv`
- `derived/entities/entity_aliases_resolved_latest.tsv`
- `derived/entities/entity_mentions_latest.tsv`
- `derived/claims/claim_quality_flags_latest.tsv`
- `derived/claims/claim_review_queue_latest.tsv`
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
4. Review `derived/claims/claim_review_queue_latest.tsv` and work `p1` items first.
5. Promote verified new facts into `derived/claims/claim_registry_latest.tsv` and evidence links.
6. Address remaining high-severity rows in `derived/claims/claim_quality_flags_latest.tsv`.
7. Commit both source changes and generated artifacts in one commit for traceability.
