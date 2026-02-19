# Derived Data Directory

This folder stores normalized outputs generated from `raw/` sources.

Current ingest outputs:
- `derived/doj_epstein_library/epstein_library_index_latest.tsv`
- `derived/doj_epstein_library/epstein_library_index_<timestamp>.tsv`
- `derived/doj_epstein_library/epstein_library_summary_latest.md`
- `derived/epstein_universe/epstein_universe_index_latest.tsv`
- `derived/epstein_universe/epstein_universe_index_<timestamp>.tsv`
- `derived/epstein_universe/epstein_universe_summary_latest.md`
- `derived/bondi_hearing/bondi_hearing_updates_latest.tsv`
- `derived/bondi_hearing/bondi_hearing_summary_latest.md`
- `derived/media_coverage/coverage_last7d_latest.tsv`
- `derived/media_coverage/media_coverage_summary_latest.md`
- `derived/media_coverage/outlet_endpoint_status_latest.tsv`
- `derived/primary_docs/primary_documents_latest.tsv`
- `derived/primary_docs/primary_documents_<timestamp>.tsv`
- `derived/primary_docs/primary_documents_summary_latest.md`
- `derived/topics/primary_doc_topic_index_latest.tsv`
- `derived/topics/topic_catalog_latest.tsv`
- `derived/topics/primary_doc_topics_summary_latest.md`
- `derived/database/epstein_research.sqlite`
- `derived/claims/claim_registry_latest.tsv`
- `derived/claims/claim_evidence_links_latest.tsv`
- `derived/claims/claim_candidates_latest.tsv`
- `derived/claims/claim_candidates_summary_latest.md`
- `derived/claims/history/claim_registry_<timestamp>.tsv`
- `derived/reports/daily_change_report_latest.md`
- `derived/reports/daily_primary_doc_diff_latest.tsv`
- `derived/reports/daily_claim_status_changes_latest.tsv`
- `derived/reports/research_command_center_latest.md`
- `derived/reports/research_command_center_latest.html`

Regenerate derived files with:

- `./scripts/ingest_primary_authority_docs.py`
- `./scripts/derive_primary_doc_topics.py`
- `./scripts/generate_claim_candidates.py`
- `./scripts/load_epstein_sqlite.py`
- `./scripts/generate_daily_change_report.py`
- `./scripts/generate_research_command_center.py`
