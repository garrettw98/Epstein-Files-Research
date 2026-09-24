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
make claim-primary-gaps
make redaction-taxonomy
make load-db
make daily-report
make coverage-gaps
make command-center
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

10. `scripts/generate_primary_evidence_gap_register.py`
   - Builds a register of claims missing tier-1 evidence links.
   - Writes `primary_evidence_gap_register_latest.tsv` and summary to `derived/reports/`.

11. `scripts/generate_redaction_taxonomy_report.py`
   - Classifies claim language into redaction rationale categories.
   - Writes `redaction_taxonomy_latest.tsv` and summary to `derived/reports/`.

12. `scripts/generate_daily_change_report.py`
   - Compares latest two primary-doc snapshots.
   - Snapshots claim registry into `derived/claims/history/`.
   - Computes claim-status diffs from latest two claim snapshots.
   - Summarizes latest claim-quality flag severities.
   - Writes reports to `derived/reports/`.

13. `scripts/generate_coverage_gap_dashboard.py`
   - Compares expected DOJ data-set range vs detected ingested set links.
   - Adds per-data-set file-count checks and zero-file set warnings.
   - Flags broken endpoints, stale inputs, and missing expected source systems.
   - Writes dashboard + metrics to `derived/reports/`.

14. `scripts/generate_research_command_center.py`
   - Builds a one-page command center view with change pulse, risk/quality summary, and navigation jump points.
   - Writes markdown and HTML outputs to `derived/reports/`.

15. `scripts/update_last24h_brief.py`
   - Builds a rolling 24-hour change brief from latest ingest/report artifacts.
   - Auto-updates managed brief blocks in `README.md` and `timeline/Full_Timeline.md`.

16. `scripts/run_daily_pipeline.sh`
   - End-to-end wrapper: ingest library, derive dataset completeness, ingest primary docs, derive topics, generate claims/entities/quality/triage, generate primary-evidence/redaction reports, load DB, generate reports, generate command center, update 24-hour brief.

## Network Requirements

Several pipeline steps fetch live sources; the rest run entirely on local `raw/` and `derived/` files. If your environment restricts outbound traffic, allow the hosts below or run only the offline steps.

| Step | Script | Needs network | Hosts contacted |
|---|---|---|---|
| Library ingest | `scripts/ingest_epstein_library.sh` | Yes | www.justice.gov |
| Data-set completeness | `scripts/derive_doj_dataset_completeness.py` | Yes | www.justice.gov |
| Primary authority ingest | `scripts/ingest_primary_authority_docs.py` | Yes | www.courtlistener.com, www.govinfo.gov, www.govtrack.us, judiciary.house.gov, www.justice.gov, oig.justice.gov |
| Universe ingest | `scripts/ingest_epstein_universe.py` | Yes | www.justice.gov, oig.justice.gov, www.govtrack.us, apnews.com, www.reuters.com, www.bbc.com, www.theguardian.com, www.foxnews.com, www.cnn.com, api.gdeltproject.org |
| Bondi hearing liveblog | `scripts/ingest_bondi_hearing_liveblog.py` | Yes | judiciary.house.gov, www.cbsnews.com |
| Media coverage map | `scripts/analyze_epstein_media_coverage.py` | Yes | Major outlet sitemaps (AP, Reuters, BBC, Guardian, Fox, CNN, CBS, ABC, NPR, NYT, WSJ) |
| Topics, claims, entities, quality, triage, gap register, redaction taxonomy, SQLite load, daily report, coverage dashboard, command center, 24h brief | `derive_primary_doc_topics.py` ... `update_last24h_brief.py` | **No** | — |
| Live events | `scripts/update_live_events.sh` | **No** (reads `updates/live_events.latest.txt`) | — |

### Offline refresh (when ingest hosts are unreachable)

```bash
./scripts/update_live_events.sh --as-of "Mon DD, YYYY" --dataset 12 --events-file updates/live_events.latest.txt
python3 scripts/derive_primary_doc_topics.py
python3 scripts/generate_claim_candidates.py
python3 scripts/derive_entity_mentions.py
python3 scripts/assess_claim_context_quality.py
python3 scripts/triage_claim_quality_flags.py
python3 scripts/generate_primary_evidence_gap_register.py
python3 scripts/generate_redaction_taxonomy_report.py
python3 scripts/load_epstein_sqlite.py
python3 scripts/generate_daily_change_report.py
python3 scripts/generate_coverage_gap_dashboard.py
python3 scripts/generate_research_command_center.py
python3 scripts/update_last24h_brief.py
```

An offline refresh re-derives outputs from the most recent `raw/` snapshots, so data-set counts and primary-document diffs will reflect the last successful ingest, not the current DOJ site. The coverage-gap dashboard's staleness warnings are expected in that case.

### Update log

- **Sep 23, 2026**: Narrative and claim-registry update covering Mar 17 - Sep 23, 2026. The environment used for this update blocked all ingest hosts listed above, so the network steps were **not** re-run; the latest `raw/` snapshots remain those from Mar 17, 2026. Offline steps were re-run against the updated claim registry.
- **Sep 24, 2026**: Verification pass. Added 14 live events and 18 claims (6 `verified_primary`, 12 `alleged`), corrected dates (Jagland charged Feb 12; Juul resigned Feb 8 and was charged Feb 9; New Mexico Truth Commission final report due Dec 31, 2026), and re-ran the offline steps. Network ingest was still blocked.
- **Sep 24, 2026 (later)**: Gap-filling passes. Added 33 live events and 30 claims (6 `verified_primary`, 24 `alleged`), new topics (survivor litigation, Massie floor list, medical network) and profiles (Fekkai, Clayton, Maurene Comey, Plaskett). Offline steps re-run; network ingest still blocked.
- **Sep 24, 2026 (final round)**: Added 8 live events and 9 claims (1 `verified_primary`, 8 `alleged`), new topic (survivor advocacy) and profile (Melania Trump), Brazil, Mexico, and Russia sections on the international page, a Harvard section on the academic page, and a fabricated-images and unverified-leak entry on the disinformation page. Corrected the date of the Vance-Carlson Situation Room meeting to Jul 17, 2025.
- **Sep 24, 2026 (final round, part 2)**: Added 8 live events and 6 claims (4 `verified_primary`, 2 `alleged`), a new topic (Senate Finance investigation, including the DEA "Chain Reaction" memo dispute and the blocked Treasury-records bill) and profile (Andrew Farkas), and Leon Black (Wyden letters, Dartmouth, MoMA), Brad Karp, Ohio State, island, polling, Australia, and Gardner-hoax updates.
- **Sep 24, 2026 (final round, part 3)**: Added 4 live events and 3 claims (all `alleged`). Covered the Jan 21 monitor denial, the Dec 2025 inherent-contempt threat, Gordon Brown's letter to the Met, Chopra's exit from UCSD, Attia's timeline, and Summers's departure. Sorted out-of-order bullets in seven profiles.

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
- `derived/reports/primary_evidence_gap_register_latest.tsv`
- `derived/reports/redaction_taxonomy_latest.tsv`
- `derived/database/epstein_research.sqlite`
- `derived/reports/daily_change_report_latest.md`
- `derived/reports/daily_primary_doc_diff_latest.tsv`
- `derived/reports/daily_claim_status_changes_latest.tsv`
- `derived/reports/coverage_gap_dashboard_latest.md`
- `derived/reports/coverage_gap_metrics_latest.tsv`
- `derived/reports/research_command_center_latest.md`
- `derived/reports/research_command_center_latest.html`

## Suggested Daily Routine

1. Run `make daily-pipeline`.
2. Review `derived/reports/daily_change_report_latest.md`.
3. Review `derived/reports/coverage_gap_dashboard_latest.md` for missing datasets/sources.
4. Review `derived/claims/claim_review_queue_latest.tsv` and work `p1` items first.
5. Promote verified new facts into `derived/claims/claim_registry_latest.tsv` and evidence links.
6. Address remaining high-severity rows in `derived/claims/claim_quality_flags_latest.tsv`.
7. Commit both source changes and generated artifacts in one commit for traceability.
