#!/usr/bin/env python3
"""Ingest Bondi House hearing primary/public coverage artifacts."""

from __future__ import annotations

import datetime as dt
import html
import json
import pathlib
import re
import sys
import urllib.request

USER_AGENT = "Mozilla/5.0 (EpsteinResearchIngest/1.0)"
CBS_URL = "https://www.cbsnews.com/live-updates/pam-bondi-hearing-epstein-files-justice-department-congress/"
HOUSE_URL = "https://judiciary.house.gov/committee-activity/hearings/oversight-us-department-justice-5"


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ts_utc() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def fetch_text(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_json_ld_blocks(html_text: str) -> list[dict]:
    blocks = re.findall(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    docs: list[dict] = []
    for block in blocks:
        payload = block.strip()
        if not payload:
            continue
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            docs.append(parsed)
        elif isinstance(parsed, list):
            docs.extend([x for x in parsed if isinstance(x, dict)])
    return docs


def clean_html_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def to_tsv_field(value: str) -> str:
    return (value or "").replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def parse_house_meta(html_text: str) -> dict[str, str]:
    date_match = re.search(r"<time[^>]+>([^<]+)</time>", html_text)
    location_match = re.search(r"Location<span[^>]*>:</span></div><div class=\"field__item\">([^<]+)</div>", html_text, flags=re.IGNORECASE)
    witness_match = re.search(r"<strong><u>WITNESS</u></strong>.*?<li><span>(.*?)</span></li>", html_text, flags=re.IGNORECASE | re.DOTALL)
    video_match = re.search(r"https://www\.youtube\.com/embed/([A-Za-z0-9_-]+)", html_text)
    return {
        "date_line": clean_html_text(date_match.group(1)) if date_match else "",
        "location": clean_html_text(location_match.group(1)) if location_match else "",
        "witness": clean_html_text(witness_match.group(1)) if witness_match else "",
        "youtube_id": (video_match.group(1).strip() if video_match else ""),
        "youtube_url": (
            f"https://www.youtube.com/watch?v={video_match.group(1).strip()}" if video_match else ""
        ),
    }


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    raw_dir = root / "raw" / "bondi_hearing"
    derived_dir = root / "derived" / "bondi_hearing"
    raw_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)

    stamp = ts_utc()
    cbs_html = fetch_text(CBS_URL)
    house_html = fetch_text(HOUSE_URL)

    cbs_html_path = raw_dir / f"{stamp}_cbs_liveblog.html"
    house_html_path = raw_dir / f"{stamp}_house_hearing.html"
    cbs_html_path.write_text(cbs_html, encoding="utf-8")
    house_html_path.write_text(house_html, encoding="utf-8")

    docs = parse_json_ld_blocks(cbs_html)
    live_doc = next((d for d in docs if d.get("@type") == "LiveBlogPosting"), {})
    updates = live_doc.get("liveBlogUpdate", []) if isinstance(live_doc, dict) else []

    house_meta = parse_house_meta(house_html)

    tsv_latest = derived_dir / "bondi_hearing_updates_latest.tsv"
    tsv_timestamp = derived_dir / f"bondi_hearing_updates_{stamp}.tsv"
    md_latest = derived_dir / "bondi_hearing_summary_latest.md"
    md_timestamp = derived_dir / f"bondi_hearing_summary_{stamp}.md"

    header = [
        "ingested_at_utc",
        "update_index",
        "published_et",
        "modified_et",
        "headline",
        "url",
        "body_text",
    ]
    with tsv_latest.open("w", encoding="utf-8") as handle:
        handle.write("\t".join(header) + "\n")
        for i, update in enumerate(updates, start=1):
            headline = to_tsv_field(str(update.get("headline", "")))
            url = to_tsv_field(str(update.get("url", "")))
            published = to_tsv_field(str(update.get("datePublished", "")))
            modified = to_tsv_field(str(update.get("dateModified", "")))
            body = to_tsv_field(clean_html_text(str(update.get("articleBody", ""))))
            handle.write(
                "\t".join(
                    [
                        stamp,
                        str(i),
                        published,
                        modified,
                        headline,
                        url,
                        body,
                    ]
                )
                + "\n"
            )
    tsv_timestamp.write_text(tsv_latest.read_text(encoding="utf-8"), encoding="utf-8")

    def write_md(path: pathlib.Path) -> None:
        with path.open("w", encoding="utf-8") as handle:
            handle.write("# Bondi Hearing Ingest Summary\n\n")
            handle.write(f"- Run UTC: {stamp}\n")
            handle.write(f"- House hearing page: {HOUSE_URL}\n")
            handle.write(f"- CBS live updates: {CBS_URL}\n")
            handle.write(f"- House date/time: {house_meta['date_line']}\n")
            handle.write(f"- House location: {house_meta['location']}\n")
            handle.write(f"- Witness listed: {house_meta['witness']}\n")
            if house_meta["youtube_url"]:
                handle.write(f"- House video: {house_meta['youtube_url']}\n")
            handle.write(f"- Update count captured: {len(updates)}\n\n")
            handle.write("## Update Chronology (CBS LiveBlog)\n")
            for i, update in enumerate(updates, start=1):
                headline = to_tsv_field(str(update.get("headline", "")))
                url = to_tsv_field(str(update.get("url", "")))
                published = to_tsv_field(str(update.get("datePublished", "")))
                handle.write(f"- {i:02d}. {published} | {headline} | {url}\n")

    write_md(md_latest)
    write_md(md_timestamp)

    print("Ingest complete.")
    print(f"- {cbs_html_path}")
    print(f"- {house_html_path}")
    print(f"- {tsv_latest}")
    print(f"- {md_latest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
