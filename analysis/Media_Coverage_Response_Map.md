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
