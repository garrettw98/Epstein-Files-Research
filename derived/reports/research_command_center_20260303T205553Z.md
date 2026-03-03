# Research Command Center

- Generated UTC: 20260303T205553Z
- Purpose: one page to see current changes, risks, and where to dive deeper.

## Snapshot

| Signal | Current value |
| :--- | :--- |
| Primary documents indexed | 41 |
| Claims tracked | 9 (alleged=6, verified_primary=3) |
| Claims lacking tier-1 evidence links | 6 (alleged=6) |
| Claim review queue (open) | p1=0, p2=0, p3=0 |
| Open claim-quality flags | high=0, warn=0, info=0 |
| DOJ dataset coverage | 12/12 sets with files, 524 files indexed |
| Coverage warnings | missing_datasets=0, media_endpoint_failures=3, stale_inputs=0 |

## Change Pulse

- Primary-doc diffs: added=3, removed=4, changed=0.
- Claim-status diffs: added=0, removed=0, changed=0.

### Primary Doc Samples
- added: `doc-govinfo-transcript-3d4ac810913a` (govinfo_wssearch)
- added: `doc-govinfo-transcript-c9be0e03d017` (govinfo_wssearch)
- added: `doc-govinfo-transcript-db958ed50987` (govinfo_wssearch)
- removed: `doc-govinfo-transcript-32517ca53042` (govinfo_wssearch)
- removed: `doc-govinfo-transcript-4dc4d5bd2bda` (govinfo_wssearch)
- removed: `doc-govinfo-transcript-715e3f2e3283` (govinfo_wssearch)
- removed: `doc-govinfo-transcript-d2204fbbbd6e` (govinfo_wssearch)

### Claim Change Samples
- No claim-status diffs found in `daily_claim_status_changes_latest.tsv`.

## Quality and Coverage Alerts

- `doj_library_broken_links` = 1. Non-200/301/302 status codes in DOJ library index.
- `media_endpoint_failures` = 3. Outlet endpoints currently non-200/301/302 from latest status file.

### Failing Endpoints
- AP: 429 (https://apnews.com/sitemap.xml)
- NYTimes: 403 (https://www.nytimes.com/sitemap.xml)
- NPR: 404 (https://www.npr.org/sitemaps/sitemap-index.xml)

## Active Entities and Topics

### Top Entities (by mention_count)
- Jeffrey Epstein: 100
- Ghislaine Maxwell: 42
- DOJ OPA: 6
- Casey Wasserman: 3
- Pam Bondi: 3

### Top Topics (by tagged_rows)
- Court and Litigation (court_and_litigation): 16
- Congressional Record Activity (congressional_record_activity): 12
- Unsealing and Access (unsealing_and_access): 11
- DOJ Release Operations (doj_release_operations): 5
- Congressional Oversight (congressional_oversight): 4
- Transparency Legislation (transparency_legislation): 3
- Unclassified Epstein Records (unclassified_epstein_records): 3
- Clemency and Pardon (clemency_and_pardon): 2

## Focus Actions

1. Backfill tier-1 links for 6 claim(s) in derived/reports/primary_evidence_gap_register_latest.tsv.
2. Review non-200 media endpoints in derived/media_coverage/outlet_endpoint_status_latest.tsv.

## Data Freshness

- Primary ingest run: 20260303T205542Z (0m ago).
- Universe ingest run: 20260303T033102Z (17.4h ago).
- Claim queue run: not reported in file (0m ago).
- Quality flags run: not reported in file (0m ago).
- Dataset completeness run: 20260303T205521Z (0m ago).

## Navigation

- What changed now: `derived/reports/daily_change_report_latest.md`
- Coverage health: `derived/reports/coverage_gap_dashboard_latest.md`
- Evidence gaps: `derived/reports/primary_evidence_gap_register_latest.md`
- Redaction patterns: `derived/reports/redaction_taxonomy_summary_latest.md`
- Source authority index: `evidence/Primary_Sources_Index.md`
- Timeline: `timeline/Full_Timeline.md`
- Government response timeline: `timeline/Government_Response_To_Epstein_Files.md`
- People index: `profiles/README.md`
- Topic map: `topics/FAQ.md`
- Core overview: `README.md`

## Taxonomy Snapshot

- Redaction categories: context_gap=2, unknown=7.
