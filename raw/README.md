# Raw Data Directory

This folder stores unmodified source snapshots.

Current ingest target:
- `raw/doj_epstein_library/` - HTML snapshots fetched from the DOJ Epstein portal.
- `raw/epstein_universe/` - Discovery artifacts for multi-source ingest:
  - `sitemap_fetch_log_latest.tsv`
  - `discovery_candidates_latest.tsv`
  - `gdelt_*.json`
  - `run_manifest_latest.json`
- `raw/bondi_hearing/` - Hearing-specific raw snapshots:
  - `*_cbs_liveblog.html`
  - `*_house_hearing.html`
- `raw/primary_docs/` - Primary authority ingest snapshots:
  - `*_courtlistener_*.json`
  - `*_courtlistener_*_recap.json` (PACER/RECAP discovery)
  - `*_house_hearing.html`
  - `*_govtrack_hr4405.html`
  - `*_govinfo_*.json` (Congressional transcript/hearing search payloads)
  - `run_manifest_latest.json`

Do not hand-edit raw files after ingest.
