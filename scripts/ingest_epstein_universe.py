#!/usr/bin/env python3
"""Multi-source Epstein ingest across government and news sitemaps."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as et
from collections import Counter
from dataclasses import dataclass

USER_AGENT = "Mozilla/5.0 (EpsteinResearchIngest/1.0)"
DEFAULT_KEYWORDS = ["epstein", "ghislaine", "jeffrey-epstein"]
DEFAULT_SOURCES = [
    ("doj", "https://www.justice.gov/sitemap.xml", 120),
    ("doj_oig", "https://oig.justice.gov/sitemap.xml", 20),
    ("ap_latest", "https://apnews.com/ap-sitemap-latest.xml", 1),
    ("ap_archive", "https://apnews.com/sitemap.xml", 36),
    ("reuters_news", "https://www.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml", 30),
    ("cnn_news", "https://www.cnn.com/sitemaps/sitemap-index.xml", 30),
    ("bbc_news", "https://www.bbc.com/sitemaps/https-index-com-news.xml", 20),
    ("guardian_news", "https://www.theguardian.com/sitemaps/news.xml", 1),
    ("fox_news", "https://www.foxnews.com/sitemap.xml", 20),
    ("govtrack", "https://www.govtrack.us/sitemap.xml", 10),
]
DEFAULT_GDELT_QUERIES = ["\"Jeffrey Epstein\"", "\"Ghislaine Maxwell\""]
URL_RE = re.compile(r"https?://[^\s)>\"]+")


@dataclass
class Discovery:
    source: str
    source_kind: str
    discovery_source: str
    discovery_detail: str
    url: str
    url_lastmod: str
    published_utc: str
    title: str
    match_terms: str


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Ingest a broad Epstein link universe.")
    parser.add_argument(
        "--raw-dir",
        default=str(root / "raw" / "epstein_universe"),
        help="Raw output directory.",
    )
    parser.add_argument(
        "--derived-dir",
        default=str(root / "derived" / "epstein_universe"),
        help="Derived output directory.",
    )
    parser.add_argument(
        "--seed-file",
        default=str(root / "evidence" / "Primary_Sources_Index.md"),
        help="Optional seed file containing URLs.",
    )
    parser.add_argument(
        "--keywords",
        default=",".join(DEFAULT_KEYWORDS),
        help="Comma-separated keyword list for filtering URLs/titles.",
    )
    parser.add_argument(
        "--max-sitemaps-per-source",
        type=int,
        default=0,
        help="Override max sitemaps fetched per source (0 = use per-source defaults).",
    )
    parser.add_argument(
        "--gdelt-maxrecords",
        type=int,
        default=250,
        help="Max records per GDELT query.",
    )
    parser.add_argument(
        "--gdelt-days-back",
        type=int,
        default=30,
        help="Only keep GDELT articles seen in the last N days.",
    )
    parser.add_argument(
        "--skip-gdelt",
        action="store_true",
        help="Skip GDELT discovery step.",
    )
    parser.add_argument(
        "--no-status-check",
        action="store_true",
        help="Skip HTTP status probing for discovered URLs.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Worker count for HTTP status probing.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Per-request timeout (seconds).",
    )
    parser.add_argument(
        "--max-urls",
        type=int,
        default=0,
        help="Optional cap for final URL set (0 = no cap).",
    )
    parser.add_argument(
        "--save-sitemaps",
        action="store_true",
        help="Store raw sitemap XML snapshots (large output).",
    )
    return parser.parse_args()


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ts_utc() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def clean_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme not in {"http", "https"}:
        return ""
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def text_to_ascii_tsv(value: str) -> str:
    return value.replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def parse_iso_date(raw: str) -> dt.datetime | None:
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


def parse_gdelt_date(raw: str) -> dt.datetime | None:
    if not raw:
        return None
    try:
        parsed = dt.datetime.strptime(raw, "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=dt.timezone.utc)


def read_url(url: str, timeout: int) -> tuple[str, int, str, str]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/xml,text/xml,application/json,text/html,*/*"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read()
        status = getattr(resp, "status", 200)
        final_url = resp.geturl()
        content_type = resp.headers.get("Content-Type", "")
    text = payload.decode("utf-8", errors="replace")
    return text, status, final_url, content_type


def fetch_text(url: str, timeout: int) -> tuple[str, int, str, str]:
    try:
        return read_url(url, timeout)
    except urllib.error.HTTPError as err:
        return "", int(err.code or 0), getattr(err, "url", url), str(err.headers or "")
    except Exception:
        return "", 0, url, ""


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def parse_sitemap(xml_text: str) -> tuple[str, list[dict[str, str]]]:
    try:
        root = et.fromstring(xml_text)
    except et.ParseError:
        return "invalid", []

    root_name = local_name(root.tag)
    items: list[dict[str, str]] = []

    if root_name == "sitemapindex":
        for child in root:
            if local_name(child.tag) != "sitemap":
                continue
            loc = ""
            lastmod = ""
            for node in child:
                name = local_name(node.tag)
                text = (node.text or "").strip()
                if name == "loc":
                    loc = text
                elif name == "lastmod":
                    lastmod = text
            if loc:
                items.append({"loc": loc, "lastmod": lastmod})
        return "index", items

    if root_name == "urlset":
        for child in root:
            if local_name(child.tag) != "url":
                continue
            loc = ""
            lastmod = ""
            title = ""
            for node in child.iter():
                name = local_name(node.tag)
                text = (node.text or "").strip()
                if not text:
                    continue
                if name == "loc" and not loc:
                    loc = text
                elif name == "lastmod" and not lastmod:
                    lastmod = text
                elif name == "title" and not title:
                    title = text
            if loc:
                items.append({"loc": loc, "lastmod": lastmod, "title": title})
        return "urlset", items

    return "other", []


def matches(url: str, title: str, keywords: list[str]) -> str:
    haystack = f"{url.lower()} {title.lower()}"
    hit = [kw for kw in keywords if kw and kw in haystack]
    return ",".join(sorted(set(hit)))


def safe_snap_name(source: str, index: int, url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"{source}_{index:04d}_{digest}.xml"


def collect_from_sitemap_source(
    source_name: str,
    root_sitemap: str,
    max_sitemaps: int,
    keywords: list[str],
    raw_snap_dir: pathlib.Path,
    save_sitemaps: bool,
    timeout: int,
) -> tuple[list[Discovery], list[dict[str, str]]]:
    discoveries: list[Discovery] = []
    fetch_log: list[dict[str, str]] = []
    queue: list[tuple[str, str]] = [(root_sitemap, root_sitemap)]
    visited: set[str] = set()
    fetched = 0

    while queue and fetched < max_sitemaps:
        sitemap_url, parent = queue.pop(0)
        sitemap_url = clean_url(sitemap_url)
        if not sitemap_url or sitemap_url in visited:
            continue
        visited.add(sitemap_url)
        fetched += 1

        xml_text, status, final_url, content_type = fetch_text(sitemap_url, timeout)
        fetch_log.append(
            {
                "source": source_name,
                "sitemap_url": sitemap_url,
                "parent_sitemap": parent,
                "status_code": str(status),
                "final_url": final_url,
                "content_type": content_type,
            }
        )

        if not xml_text:
            continue

        if save_sitemaps:
            snapshot = raw_snap_dir / safe_snap_name(source_name, fetched, sitemap_url)
            snapshot.write_text(xml_text, encoding="utf-8")

        kind, items = parse_sitemap(xml_text)
        if kind == "index":
            indexed = []
            for order, item in enumerate(items):
                loc = clean_url(item.get("loc", ""))
                if not loc:
                    continue
                parsed = parse_iso_date(item.get("lastmod", ""))
                indexed.append((parsed or dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc), order, loc))
            indexed.sort(key=lambda row: row[0], reverse=True)
            for _, _, loc in indexed:
                if source_name == "doj" and "sitemap.xml?page=" in loc and not loc.endswith("page=0"):
                    continue
                if loc not in visited:
                    queue.append((loc, sitemap_url))
        elif kind == "urlset":
            for item in items:
                url = clean_url(item.get("loc", ""))
                if not url:
                    continue
                title = item.get("title", "")
                hit = matches(url, title, keywords)
                if not hit:
                    continue
                discoveries.append(
                    Discovery(
                        source=source_name,
                        source_kind="sitemap",
                        discovery_source=root_sitemap,
                        discovery_detail=sitemap_url,
                        url=url,
                        url_lastmod=item.get("lastmod", ""),
                        published_utc="",
                        title=title,
                        match_terms=hit,
                    )
                )

    return discoveries, fetch_log


def collect_seed_urls(seed_file: pathlib.Path) -> list[Discovery]:
    if not seed_file.exists():
        return []
    payload = seed_file.read_text(encoding="utf-8", errors="replace")
    found = []
    for raw in URL_RE.findall(payload):
        cleaned = clean_url(raw.rstrip(".,;:"))
        if not cleaned:
            continue
        found.append(
            Discovery(
                source="seed_primary_sources",
                source_kind="seed_file",
                discovery_source=str(seed_file),
                discovery_detail="seed_url",
                url=cleaned,
                url_lastmod="",
                published_utc="",
                title="",
                match_terms="seed",
            )
        )
    return found


def collect_gdelt(
    keywords: list[str],
    raw_dir: pathlib.Path,
    timeout: int,
    max_records: int,
    days_back: int,
) -> list[Discovery]:
    discoveries: list[Discovery] = []
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"
    cutoff = now_utc() - dt.timedelta(days=days_back)

    for query in DEFAULT_GDELT_QUERIES:
        params = urllib.parse.urlencode(
            {
                "query": query,
                "mode": "ArtList",
                "maxrecords": str(max_records),
                "format": "json",
                "sort": "DateDesc",
            }
        )
        url = f"{endpoint}?{params}"
        payload, status, final_url, _ = fetch_text(url, timeout)
        stamp = re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_")
        (raw_dir / f"gdelt_{stamp}.json").write_text(payload or "", encoding="utf-8")
        if status >= 400 or not payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        for article in data.get("articles", []):
            article_url = clean_url(str(article.get("url", "")))
            if not article_url:
                continue
            title = str(article.get("title", "")).strip()
            seen = parse_gdelt_date(str(article.get("seendate", "")))
            if seen and seen < cutoff:
                continue
            hit = matches(article_url, title, keywords)
            if not hit:
                continue
            discoveries.append(
                Discovery(
                    source="gdelt",
                    source_kind="news_feed",
                    discovery_source=endpoint,
                    discovery_detail=f"{query} ({final_url})",
                    url=article_url,
                    url_lastmod="",
                    published_utc=seen.isoformat() if seen else "",
                    title=title,
                    match_terms=hit,
                )
            )
    return discoveries


def probe_status(url: str, timeout: int) -> tuple[str, str, str]:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return str(getattr(resp, "status", 200)), resp.geturl(), resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as err:
        return str(int(err.code or 0)), getattr(err, "url", url), str(err.headers.get("Content-Type", ""))
    except Exception:
        pass

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Range": "bytes=0-512"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read(1)
            return str(getattr(resp, "status", 200)), resp.geturl(), resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as err:
        return str(int(err.code or 0)), getattr(err, "url", url), str(err.headers.get("Content-Type", ""))
    except Exception:
        return "000", url, ""


def merge_discoveries(items: list[Discovery]) -> list[Discovery]:
    merged: dict[str, Discovery] = {}
    for item in items:
        key = item.url
        existing = merged.get(key)
        if not existing:
            merged[key] = item
            continue

        source_set = set(existing.source.split(",")) | set(item.source.split(","))
        existing.source = ",".join(sorted(x for x in source_set if x))

        kind_set = set(existing.source_kind.split(",")) | set(item.source_kind.split(","))
        existing.source_kind = ",".join(sorted(x for x in kind_set if x))

        detail_set = set(existing.discovery_detail.split(" || ")) | set(item.discovery_detail.split(" || "))
        existing.discovery_detail = " || ".join(sorted(x for x in detail_set if x))

        terms = set(existing.match_terms.split(",")) | set(item.match_terms.split(","))
        existing.match_terms = ",".join(sorted(x for x in terms if x))

        if not existing.title and item.title:
            existing.title = item.title
        if item.url_lastmod and item.url_lastmod > existing.url_lastmod:
            existing.url_lastmod = item.url_lastmod
        if not existing.published_utc and item.published_utc:
            existing.published_utc = item.published_utc
    return sorted(merged.values(), key=lambda row: row.url)


def write_fetch_log(path: pathlib.Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write("source\tsitemap_url\tparent_sitemap\tstatus_code\tfinal_url\tcontent_type\n")
        for row in rows:
            handle.write(
                "\t".join(
                    text_to_ascii_tsv(row.get(col, ""))
                    for col in ["source", "sitemap_url", "parent_sitemap", "status_code", "final_url", "content_type"]
                )
                + "\n"
            )


def write_discovery_log(path: pathlib.Path, rows: list[Discovery]) -> None:
    header = [
        "source",
        "source_kind",
        "discovery_source",
        "discovery_detail",
        "url",
        "url_lastmod",
        "published_utc",
        "title",
        "match_terms",
    ]
    with path.open("w", encoding="utf-8") as handle:
        handle.write("\t".join(header) + "\n")
        for row in rows:
            handle.write(
                "\t".join(
                    text_to_ascii_tsv(value)
                    for value in [
                        row.source,
                        row.source_kind,
                        row.discovery_source,
                        row.discovery_detail,
                        row.url,
                        row.url_lastmod,
                        row.published_utc,
                        row.title,
                        row.match_terms,
                    ]
                )
                + "\n"
            )


def write_index(
    path: pathlib.Path,
    run_utc: str,
    rows: list[Discovery],
    statuses: dict[str, tuple[str, str, str]],
) -> None:
    header = [
        "ingested_at_utc",
        "source",
        "source_kind",
        "discovery_source",
        "discovery_detail",
        "url",
        "url_lastmod",
        "published_utc",
        "title",
        "match_terms",
        "status_code",
        "final_url",
        "content_type",
    ]
    with path.open("w", encoding="utf-8") as handle:
        handle.write("\t".join(header) + "\n")
        for row in rows:
            status_code, final_url, content_type = statuses.get(row.url, ("unchecked", "", ""))
            values = [
                run_utc,
                row.source,
                row.source_kind,
                row.discovery_source,
                row.discovery_detail,
                row.url,
                row.url_lastmod,
                row.published_utc,
                row.title,
                row.match_terms,
                status_code,
                final_url,
                content_type,
            ]
            handle.write("\t".join(text_to_ascii_tsv(value) for value in values) + "\n")


def write_summary(path: pathlib.Path, run_utc: str, rows: list[Discovery], statuses: dict[str, tuple[str, str, str]]) -> None:
    status_counts = Counter()
    source_counts = Counter()
    domain_counts = Counter()
    weekly = []
    cutoff = now_utc() - dt.timedelta(days=7)

    for row in rows:
        status_counts[statuses.get(row.url, ("unchecked", "", ""))[0]] += 1
        for source in row.source.split(","):
            if source:
                source_counts[source] += 1
        domain_counts[urllib.parse.urlsplit(row.url).netloc] += 1

        date = parse_iso_date(row.published_utc) or parse_iso_date(row.url_lastmod)
        if date and date >= cutoff:
            weekly.append((date, row))

    weekly.sort(reverse=True, key=lambda pair: pair[0])

    with path.open("w", encoding="utf-8") as handle:
        handle.write("# Epstein Universe Ingest Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- URL count: {len(rows)}\n")
        handle.write(f"- Last 7-day candidates: {len(weekly)}\n\n")

        handle.write("## By Source\n")
        for key, value in source_counts.most_common():
            handle.write(f"- {key}: {value}\n")
        handle.write("\n## By HTTP Status\n")
        for key, value in sorted(status_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            handle.write(f"- {key}: {value}\n")
        handle.write("\n## Top Domains\n")
        for key, value in domain_counts.most_common(15):
            handle.write(f"- {key}: {value}\n")

        handle.write("\n## Recent 7-Day Candidates (Top 25)\n")
        for date, row in weekly[:25]:
            title = text_to_ascii_tsv(row.title) or "(no title)"
            handle.write(f"- {date.date().isoformat()} | {title} | {row.url}\n")


def main() -> int:
    args = parse_args()
    run_utc = ts_utc()

    keywords = [kw.strip().lower() for kw in args.keywords.split(",") if kw.strip()]
    if not keywords:
        keywords = DEFAULT_KEYWORDS

    raw_dir = pathlib.Path(args.raw_dir).resolve()
    derived_dir = pathlib.Path(args.derived_dir).resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)

    raw_snapshot_dir = raw_dir / f"sitemaps_{run_utc}"
    if args.save_sitemaps:
        raw_snapshot_dir.mkdir(parents=True, exist_ok=True)

    all_discoveries: list[Discovery] = []
    fetch_log: list[dict[str, str]] = []

    for source_name, root_sitemap, default_limit in DEFAULT_SOURCES:
        limit = args.max_sitemaps_per_source if args.max_sitemaps_per_source > 0 else default_limit
        discovered, fetched = collect_from_sitemap_source(
            source_name=source_name,
            root_sitemap=root_sitemap,
            max_sitemaps=limit,
            keywords=keywords,
            raw_snap_dir=raw_snapshot_dir,
            save_sitemaps=args.save_sitemaps,
            timeout=args.timeout,
        )
        all_discoveries.extend(discovered)
        fetch_log.extend(fetched)

    seed_file = pathlib.Path(args.seed_file).resolve()
    all_discoveries.extend(collect_seed_urls(seed_file))

    if not args.skip_gdelt:
        all_discoveries.extend(
            collect_gdelt(
                keywords=keywords,
                raw_dir=raw_dir,
                timeout=args.timeout,
                max_records=args.gdelt_maxrecords,
                days_back=args.gdelt_days_back,
            )
        )

    merged = merge_discoveries(all_discoveries)
    if args.max_urls > 0:
        merged = merged[: args.max_urls]

    statuses: dict[str, tuple[str, str, str]] = {}
    if args.no_status_check:
        statuses = {row.url: ("unchecked", "", "") for row in merged}
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            future_map = {pool.submit(probe_status, row.url, args.timeout): row.url for row in merged}
            for future in concurrent.futures.as_completed(future_map):
                url = future_map[future]
                try:
                    statuses[url] = future.result()
                except Exception:
                    statuses[url] = ("000", url, "")

    fetch_log_latest = raw_dir / "sitemap_fetch_log_latest.tsv"
    fetch_log_timestamp = raw_dir / f"sitemap_fetch_log_{run_utc}.tsv"
    write_fetch_log(fetch_log_latest, fetch_log)
    write_fetch_log(fetch_log_timestamp, fetch_log)
    discovery_log_latest = raw_dir / "discovery_candidates_latest.tsv"
    discovery_log_timestamp = raw_dir / f"discovery_candidates_{run_utc}.tsv"
    write_discovery_log(discovery_log_latest, merged)
    write_discovery_log(discovery_log_timestamp, merged)

    index_latest = derived_dir / "epstein_universe_index_latest.tsv"
    index_timestamp = derived_dir / f"epstein_universe_index_{run_utc}.tsv"
    summary_latest = derived_dir / "epstein_universe_summary_latest.md"
    summary_timestamp = derived_dir / f"epstein_universe_summary_{run_utc}.md"
    write_index(index_latest, run_utc, merged, statuses)
    write_index(index_timestamp, run_utc, merged, statuses)
    write_summary(summary_latest, run_utc, merged, statuses)
    write_summary(summary_timestamp, run_utc, merged, statuses)

    manifest = {
        "run_utc": run_utc,
        "keyword_count": len(keywords),
        "keywords": keywords,
        "discovered_urls": len(merged),
        "sources": [name for name, _, _ in DEFAULT_SOURCES],
        "seed_file": str(seed_file),
        "skip_gdelt": bool(args.skip_gdelt),
        "save_sitemaps": bool(args.save_sitemaps),
    }
    (raw_dir / "run_manifest_latest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (raw_dir / f"run_manifest_{run_utc}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Ingest complete.")
    print(f"- {index_latest}")
    print(f"- {index_timestamp}")
    print(f"- {summary_latest}")
    print(f"- {summary_timestamp}")
    print(f"- {fetch_log_latest}")
    print(f"- URL count: {len(merged)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
