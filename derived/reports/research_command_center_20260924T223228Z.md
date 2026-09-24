# Research Command Center

- Generated UTC: 20260924T223228Z
- Purpose: one page to see current changes, risks, and where to dive deeper.

## Snapshot

| Signal | Current value |
| :--- | :--- |
| Primary documents indexed | 46 |
| Claims tracked | 101 (alleged=68, verified_primary=33) |
| Claims lacking tier-1 evidence links | 67 (alleged=67) |
| Claim review queue (open) | p1=0, p2=0, p3=0 |
| Open claim-quality flags | high=0, warn=0, info=0 |
| DOJ dataset coverage | 12/12 sets with files, 524 files indexed |
| Coverage warnings | missing_datasets=0, media_endpoint_failures=3, stale_inputs=3 |

## Change Pulse

- Primary-doc diffs: added=8, removed=2, changed=0.
- Claim-status diffs: added=9, removed=0, changed=1.

### Primary Doc Samples
- added: `doc-govinfo-transcript-14f372471989` (govinfo_wssearch)
- added: `doc-govinfo-transcript-4dc4d5bd2bda` (govinfo_wssearch)
- added: `doc-govinfo-transcript-5f5797d2fdf5` (govinfo_wssearch)
- added: `doc-govinfo-transcript-69c3f9d9ce3c` (govinfo_wssearch)
- added: `doc-govinfo-transcript-715e3f2e3283` (govinfo_wssearch)
- added: `doc-govinfo-transcript-9d188acbe4bb` (govinfo_wssearch)
- added: `doc-govinfo-transcript-c771c1d91c83` (govinfo_wssearch)
- added: `doc-recap-8f75e9493807` (courtlistener_recap)

### Claim Change Samples
- added: `claim-brazil-mpf-epstein-inquiry-20260210` ( -> alleged)
- added: `claim-harvard-affiliates-open-letter-20260723` ( -> alleged)
- added: `claim-harvard-hms-donor-rating-20260501` ( -> alleged)
- added: `claim-mcsweeney-resigns-20260208` ( -> alleged)
- added: `claim-melania-sanctions-motion-20260721` ( -> alleged)
- added: `claim-reading-room-nyc-20260508` ( -> alleged)
- changed: `claim-regime-change-sitroom-202606` (alleged -> alleged)
- added: `claim-s5005-efta2-senate-20260715` ( -> verified_primary)

## Quality and Coverage Alerts

- `media_endpoint_failures` = 3. Outlet endpoints currently non-200/301/302 from latest status file.
- `stale_inputs` = 3. raw/primary_docs/run_manifest_latest.json (4604.2h old); raw/epstein_universe/run_manifest_latest.json (4604.2h old); derived/doj_epstein_library/epstein_library_index_latest.tsv (4604.2h old)

### Failing Endpoints
- AP: 429 (https://apnews.com/sitemap.xml)
- NYTimes: 403 (https://www.nytimes.com/sitemap.xml)
- NPR: 404 (https://www.npr.org/sitemaps/sitemap-index.xml)

## Active Entities and Topics

### Top Entities (by mention_count)
- Jeffrey Epstein: 147
- Ghislaine Maxwell: 51
- Washington Post: 18
- United States: 12
- Leon Black: 9
- The Hill: 9
- New York: 8
- Courthouse News: 8

### Top Topics (by tagged_rows)
- Congressional Record Activity (congressional_record_activity): 16
- Court and Litigation (court_and_litigation): 15
- Unsealing and Access (unsealing_and_access): 11
- DOJ Release Operations (doj_release_operations): 5
- Unclassified Epstein Records (unclassified_epstein_records): 5
- Congressional Oversight (congressional_oversight): 4
- Transparency Legislation (transparency_legislation): 3
- Clemency and Pardon (clemency_and_pardon): 2

## Focus Actions

1. Backfill tier-1 links for 67 claim(s) in derived/reports/primary_evidence_gap_register_latest.tsv.
2. Refresh stale ingest artifacts by running make daily-pipeline.
3. Review non-200 media endpoints in derived/media_coverage/outlet_endpoint_status_latest.tsv.

## Data Freshness

- Primary ingest run: 20260317T021929Z (191.8d ago).
- Universe ingest run: 20260317T021844Z (191.8d ago).
- Claim queue run: not reported in file (0m ago).
- Quality flags run: not reported in file (0m ago).
- Dataset completeness run: 20260317T021916Z (191.8d ago).

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

- Redaction categories: context_gap=6, national_security=1, unknown=84, victim_privacy=10.
