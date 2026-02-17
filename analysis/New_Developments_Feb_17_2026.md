# New Developments Deep Dive (As of Feb 17, 2026)

## Scope

This memo captures newly surfaced developments after the Feb 15 update cycle and maps what was added to the repository.

## Newly Confirmed Items Added

1. **French inquiry activity linked to Jack Lang (Feb 16, 2026)**
   - Reuters reported searches at the Arab World Institute and properties linked to Lang.
   - Reuters and BBC reported Lang denied wrongdoing.
   - Source:
     - https://www.reuters.com/world/french-police-raid-arab-world-institute-epstein-linked-probe-into-jack-lang-2026-02-16/
     - https://www.bbc.com/news/articles/c8eg7rllgl7o

2. **Tom Pritzker step-down announcement (Feb 16, 2026)**
   - Reuters reported Hyatt's executive chairman said he would step down by end of 2026 and described his Epstein ties as "terrible judgment."
   - Source:
     - https://www.reuters.com/sustainability/hyatt-executive-chairman-pritzker-steps-down-cites-terrible-judgment-epstein-2026-02-16/

3. **New Mexico Zorro Ranch commission approval (Feb 17, 2026)**
   - Reuters reported New Mexico lawmakers approved a commission process to investigate allegations tied to Zorro Ranch.
   - Source:
     - https://www.reuters.com/world/us/new-mexico-approves-comprehensive-probe-epsteins-zorro-ranch-2026-02-17/

## Repository Updates Mapped

- Live updates ingest source updated: `updates/live_events.latest.txt`
- Managed live blocks refreshed:
  - `README.md`
  - `evidence/2026_Release.md`
  - `timeline/Full_Timeline.md`
- New profile added:
  - `profiles/Jack_Lang.md`
- Existing profile updated:
  - `profiles/Tom_Pritzker.md`
- Investigation topics updated:
  - `topics/International_Investigations.md`
  - `topics/The_French_Connection.md`
- Government response chronology updated:
  - `timeline/Government_Response_To_Epstein_Files.md`
- Source index updated:
  - `evidence/Primary_Sources_Index.md`

## Deep-Dive Completeness Improvements Added

1. **DOJ ingest hardening** (`scripts/ingest_epstein_library.sh`)
   - Added fallback fetch of `/epstein/doj-disclosures` to avoid bot-interstitial-only snapshots.
   - Added filtering for social-share links to reduce false broken-link noise.
   - Result: dataset pages 1-12 are now detected directly in the ingest index.

2. **Coverage-gap accuracy restored**
   - `derived/reports/coverage_gap_dashboard_latest.md` now reports:
     - Ingested dataset numbers: 1-12
     - Missing datasets: none
     - Broken DOJ links: 0

## Remaining High-Value Gaps

- **AP endpoint reliability**: current ingest gets HTTP 429 from AP sitemap, reducing direct AP coverage consistency.
- **Claim quality backlog**: 3 high-severity and 6 warning-level claim-quality flags still require manual adjudication and evidence upgrades.
- **Dataset document-depth metrics**: current DOJ ingest now catches set pages, but not per-set expected-vs-observed document completeness counts.
