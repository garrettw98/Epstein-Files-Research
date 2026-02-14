#!/usr/bin/env python3
"""Analyze recent Epstein coverage by outlet from local ingest outputs."""

from __future__ import annotations

import csv
import datetime as dt
import pathlib
import urllib.parse
import urllib.error
import urllib.request
from collections import Counter

USER_AGENT = "Mozilla/5.0 (EpsteinResearchCoverage/1.0)"

OUTLET_ENDPOINTS = [
    ("AP", "https://apnews.com/sitemap.xml"),
    ("Reuters", "https://www.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml"),
    ("BBC", "https://www.bbc.com/sitemaps/https-index-com-news.xml"),
    ("Guardian", "https://www.theguardian.com/sitemaps/news.xml"),
    ("Fox News", "https://www.foxnews.com/sitemap.xml"),
    ("CNN", "https://www.cnn.com/sitemaps/sitemap-index.xml"),
    ("NYTimes", "https://www.nytimes.com/sitemap.xml"),
    ("WSJ", "https://www.wsj.com/sitemap.xml"),
    ("NPR", "https://www.npr.org/sitemaps/sitemap-index.xml"),
    ("CBS", "https://www.cbsnews.com/live-updates/pam-bondi-hearing-epstein-files-justice-department-congress/"),
    ("ABC News", "https://abcnews.go.com/sitemap"),
]


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_iso(raw: str) -> dt.datetime | None:
    if not raw:
        return None
    value = raw.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def http_status(url: str, timeout: int = 15) -> int:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(getattr(resp, "status", 200))
    except urllib.error.HTTPError as err:
        return int(err.code or 0)
    except Exception:
        return 0


def read_bondi_count(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    return len(rows)


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    index_path = root / "derived" / "epstein_universe" / "epstein_universe_index_latest.tsv"
    bondi_path = root / "derived" / "bondi_hearing" / "bondi_hearing_updates_latest.tsv"
    out_dir = root / "derived" / "media_coverage"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_utc = now_utc().strftime("%Y%m%dT%H%M%SZ")
    cutoff = now_utc() - dt.timedelta(days=7)

    recent_rows: list[dict[str, str]] = []
    with index_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            date = parse_iso(row.get("published_utc", "")) or parse_iso(row.get("url_lastmod", ""))
            if not date or date < cutoff:
                continue
            row["normalized_date_utc"] = date.isoformat()
            recent_rows.append(row)

    by_domain = Counter()
    samples: dict[str, str] = {}
    first_seen: dict[str, str] = {}
    last_seen: dict[str, str] = {}
    for row in recent_rows:
        url = row.get("url", "")
        domain = urllib.parse.urlsplit(url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        by_domain[domain] += 1
        samples.setdefault(domain, url)
        date = row.get("normalized_date_utc", "")
        if domain not in first_seen or date < first_seen[domain]:
            first_seen[domain] = date
        if domain not in last_seen or date > last_seen[domain]:
            last_seen[domain] = date

    coverage_latest = out_dir / "coverage_last7d_latest.tsv"
    coverage_stamp = out_dir / f"coverage_last7d_{run_utc}.tsv"
    with coverage_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["domain", "url_count_last7d", "first_seen_utc", "last_seen_utc", "sample_url"])
        for domain, count in by_domain.most_common():
            writer.writerow([domain, count, first_seen.get(domain, ""), last_seen.get(domain, ""), samples.get(domain, "")])
    coverage_stamp.write_text(coverage_latest.read_text(encoding="utf-8"), encoding="utf-8")

    outlet_status_rows = []
    for outlet, endpoint in OUTLET_ENDPOINTS:
        outlet_status_rows.append((outlet, endpoint, http_status(endpoint)))

    outlet_status_latest = out_dir / "outlet_endpoint_status_latest.tsv"
    outlet_status_stamp = out_dir / f"outlet_endpoint_status_{run_utc}.tsv"
    with outlet_status_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["outlet", "endpoint", "http_status"])
        writer.writerows(outlet_status_rows)
    outlet_status_stamp.write_text(outlet_status_latest.read_text(encoding="utf-8"), encoding="utf-8")

    bondi_update_count = read_bondi_count(bondi_path)

    summary_latest = out_dir / "media_coverage_summary_latest.md"
    summary_stamp = out_dir / f"media_coverage_summary_{run_utc}.md"
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Media Coverage Summary (Last 7 Days)\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Window: {(cutoff.date()).isoformat()} to {(now_utc().date()).isoformat()}\n")
        handle.write(f"- URLs in monitored ingest window: {len(recent_rows)}\n")
        handle.write(f"- Unique domains in monitored ingest window: {len(by_domain)}\n")
        handle.write(f"- CBS Bondi liveblog updates captured: {bondi_update_count}\n\n")
        handle.write("## Top Domains by URL Count (from monitored ingest)\n")
        for domain, count in by_domain.most_common(20):
            handle.write(f"- {domain}: {count}\n")
        handle.write("\n## Outlet Endpoint Accessibility Checks\n")
        for outlet, endpoint, status in outlet_status_rows:
            handle.write(f"- {outlet}: HTTP {status} ({endpoint})\n")
        handle.write("\n## Notes\n")
        handle.write("- This report reflects the repository's monitored-source ingest, not all global media.\n")
        handle.write("- HTTP endpoint accessibility affects whether an outlet can be automatically monitored.\n")

    summary_stamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")
    print("Coverage analysis complete.")
    print(f"- {coverage_latest}")
    print(f"- {outlet_status_latest}")
    print(f"- {summary_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
