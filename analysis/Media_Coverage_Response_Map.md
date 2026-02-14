# Media Coverage Map: Who Covered the Epstein Files and Who Is Hard to Observe

## Method Used (Repository-Ingest Based)

- Coverage window: last 7 days ending **Feb 14, 2026**.
- Primary dataset: `derived/epstein_universe/epstein_universe_index_latest.tsv`.
- Supplement for Bondi hearing detail: `derived/bondi_hearing/bondi_hearing_updates_latest.tsv`.
- Outlet endpoint checks: `derived/media_coverage/outlet_endpoint_status_latest.tsv`.

## Outlets With Strong Observable Coverage in This Window

- **AP**: 64 URLs in monitored ingest window.
- **Fox News**: 26 URLs in monitored ingest window.
- **BBC**: 19 URLs in monitored ingest window.
- **Guardian**: 16 URLs in monitored ingest window.
- **Reuters**: 10 URLs in monitored ingest window.
- **CBS (Bondi hearing)**: 23 live-blog updates captured in the hearing-specific ingest.

Source artifact: `derived/media_coverage/coverage_last7d_latest.tsv`

## Outlets Not Reliably Observable in Current Automation (or Low Visibility)

- **NYTimes**: endpoint returned HTTP 403 for sitemap access in this run.
- **NPR**: tested sitemap index endpoint returned HTTP 404.
- **CNN**: endpoint status was unstable in automated checks; no stable high-volume extraction in this run.
- **WSJ**: sitemap root reachable, but automated extraction did not produce comparable matched-output volume in this run.

Source artifact: `derived/media_coverage/outlet_endpoint_status_latest.tsv`

## Why "Who Isn't Covering" Is Hard (Important Constraint)

- Automated monitoring depends on machine-readable endpoints (sitemaps, feeds, or accessible search pages).
- Some outlets block automation, rate-limit, or require interactive rendering/paywall traversal.
- Coverage can exist without being capturable in a feed-driven ingest.

Because of those constraints, this repository can defensibly state **"not observed in monitored output"** but cannot prove **"not covered at all"** without manual outlet-by-outlet editorial review.

## Practical Interpretation

- Coverage is broad across major wire/international outlets in monitored data.
- Bondi-hearing-specific coverage is dense in CBS live updates plus AP follow-up.
- The largest blind spots are technical observability limits, not proof of editorial suppression by themselves.
