# Media Coverage Map: Who Covered the Epstein Files and What We Still Cannot Reliably Observe

## Method Used (Repository Ingest)

- Coverage window: last 7 days ending **Feb 17, 2026**.
- Primary dataset: `derived/epstein_universe/epstein_universe_index_latest.tsv`.
- Supplemental hearing stream: `derived/bondi_hearing/bondi_hearing_updates_latest.tsv`.
- Endpoint checks: `derived/media_coverage/outlet_endpoint_status_latest.tsv`.

## Strong Observable Coverage in Current Window

- **AP**: 51 URLs in monitored ingest window.
- **Fox News**: 20 URLs in monitored ingest window.
- **Guardian**: 7 URLs in monitored ingest window.
- **BBC**: 7 URLs in monitored ingest window.
- **Reuters**: 3 URLs in monitored ingest window.
- **CBS Bondi liveblog**: 23 captured updates.

Source artifact: `derived/media_coverage/coverage_last7d_latest.tsv`

## Outlets with Monitoring Friction (Technical, Not Editorial Proof)

- **AP**: HTTP 429 on sitemap endpoint in this run.
- **NYTimes**: HTTP 403 on sitemap endpoint.
- **NPR**: HTTP 404 on tested sitemap endpoint.

Source artifact: `derived/media_coverage/outlet_endpoint_status_latest.tsv`

## Why "Who Isn't Covering" Requires Caution

- Feed/sitemap monitoring only captures what endpoints expose.
- Some outlets are rate-limited, paywalled, or script-rendered in ways that reduce machine-readability.
- A low observed count can indicate ingestion limits, not absence of reporting.

## Practical Read

- Coverage remains broad across wire and major international outlets.
- The strongest near-real-time blind spots are endpoint accessibility and rate limits.
- Claims about suppression should remain separate from endpoint observability limitations unless independently corroborated.

## Sep 23, 2026 Update: Source Mix for the Mar 17 - Sep 23 Window

The automated media-coverage ingest (`scripts/analyze_epstein_media_coverage.py`, fed by `scripts/ingest_epstein_universe.py`) **could not be re-run** for this update: the execution environment's network policy blocked every outlet and GDELT endpoint. The counts in the sections above therefore still describe the February window.

As a manual substitute, the 75 events added to `updates/live_events.latest.txt` for Mar 17 - Sep 23 cite these domains (one URL per event):

| Domain | Events cited |
| :--- | ---: |
| cnn.com (incl. edition.cnn.com) | 12 |
| npr.org | 9 |
| oversight.house.gov / oversightdemocrats.house.gov / massie.house.gov | 7 |
| axios.com | 5 |
| cnbc.com | 4 |
| usnews.com (Reuters wire) | 4 |
| abcnews.com | 4 |
| cbsnews.com | 3 |
| aljazeera.com, washingtonpost.com, pbs.org, bloomberg.com | 2 each |
| Other (government, court-tracking, trade, and regional outlets) | 1 each |

**How to read this**: the table reflects which sources were reachable and most specific for each event during research, not which outlets covered the story most. AP and Reuters direct pages were reachable only indirectly (Reuters via US News syndication), which likely under-counts both. Conservative-leaning outlets (Daily Caller, Washington Examiner, Daily Signal, Fox News) also covered the September Leon Black contempt vote, the EFTA II discharge petition, and Chairman Comer's legislation.
