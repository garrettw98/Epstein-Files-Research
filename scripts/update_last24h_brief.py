#!/usr/bin/env python3
"""Generate and inject a last-24-hours brief into README and timeline files."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
import re
import urllib.parse
from collections import Counter


README_START = "<!-- LAST24H:START -->"
README_END = "<!-- LAST24H:END -->"
TIMELINE_START = "<!-- LAST24H_TIMELINE:START -->"
TIMELINE_END = "<!-- LAST24H_TIMELINE:END -->"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_ts(value: str) -> dt.datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def replace_or_append_block(text: str, start: str, end: str, new_block: str) -> str:
    pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), flags=re.DOTALL)
    if pattern.search(text):
        return pattern.sub(new_block, text)
    if text and not text.endswith("\n"):
        text += "\n"
    return text + "\n" + new_block + "\n"


def domain_for_url(url: str) -> str:
    host = urllib.parse.urlsplit(url or "").netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def to_title(value: str, fallback_url: str) -> str:
    text = (value or "").strip()
    if text:
        return text
    path = urllib.parse.urlsplit(fallback_url).path.strip("/")
    if not path:
        return "(untitled)"
    return path.rsplit("/", 1)[-1]


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Update last-24-hours brief blocks in README and timeline.")
    parser.add_argument(
        "--readme-file",
        default=str(root / "README.md"),
        help="README file path.",
    )
    parser.add_argument(
        "--timeline-file",
        default=str(root / "timeline" / "Full_Timeline.md"),
        help="Timeline file path.",
    )
    parser.add_argument(
        "--events-tsv",
        default=str(root / "derived" / "epstein_universe" / "epstein_universe_index_latest.tsv"),
        help="Universe events TSV path.",
    )
    parser.add_argument(
        "--primary-diff-tsv",
        default=str(root / "derived" / "reports" / "daily_primary_doc_diff_latest.tsv"),
        help="Primary diff TSV path.",
    )
    parser.add_argument(
        "--claim-diff-tsv",
        default=str(root / "derived" / "reports" / "daily_claim_status_changes_latest.tsv"),
        help="Claim diff TSV path.",
    )
    parser.add_argument(
        "--review-queue-tsv",
        default=str(root / "derived" / "claims" / "claim_review_queue_latest.tsv"),
        help="Claim review queue TSV path.",
    )
    parser.add_argument(
        "--dataset-counts-tsv",
        default=str(root / "derived" / "doj_epstein_library" / "dataset_file_counts_latest.tsv"),
        help="Dataset file counts TSV path.",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Rolling time window in hours.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=8,
        help="Max recent items to list.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = utc_now()
    window_start = now - dt.timedelta(hours=max(1, args.hours))

    readme_path = pathlib.Path(args.readme_file).resolve()
    timeline_path = pathlib.Path(args.timeline_file).resolve()

    events_rows = read_tsv(pathlib.Path(args.events_tsv).resolve())
    primary_diff_rows = read_tsv(pathlib.Path(args.primary_diff_tsv).resolve())
    claim_diff_rows = read_tsv(pathlib.Path(args.claim_diff_tsv).resolve())
    review_queue_rows = read_tsv(pathlib.Path(args.review_queue_tsv).resolve())
    dataset_rows = read_tsv(pathlib.Path(args.dataset_counts_tsv).resolve())

    recent_events: list[dict[str, str]] = []
    for row in events_rows:
        status_code = (row.get("status_code") or "").strip()
        if status_code and status_code not in {"200", "301", "302"}:
            continue
        ts = parse_ts((row.get("url_lastmod") or "").strip()) or parse_ts((row.get("published_utc") or "").strip())
        if not ts or ts < window_start:
            continue
        url = (row.get("final_url") or "").strip() or (row.get("url") or "").strip()
        if not url:
            continue
        recent_events.append(
            {
                "dt": ts.isoformat(),
                "date": ts.strftime("%b %d, %Y"),
                "title": to_title((row.get("title") or "").strip(), url),
                "url": url,
                "source": (row.get("source") or "").strip(),
                "domain": domain_for_url(url),
            }
        )

    recent_events.sort(key=lambda item: item["dt"], reverse=True)
    deduped_events: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for event in recent_events:
        if event["url"] in seen_urls:
            continue
        seen_urls.add(event["url"])
        deduped_events.append(event)
    recent_events = deduped_events[: max(1, args.max_items)]

    domain_counts = Counter(event["domain"] for event in deduped_events)

    primary_counts = Counter((row.get("change_type") or "").strip().lower() for row in primary_diff_rows)
    claim_counts = Counter((row.get("change_type") or "").strip().lower() for row in claim_diff_rows)

    queue_counts = Counter(
        (row.get("priority") or "").strip().lower()
        for row in review_queue_rows
        if (row.get("triage_status") or "").strip().lower() in {"", "open", "in_review"}
    )

    dataset_total_sets = len(dataset_rows)
    dataset_sets_with_files = 0
    dataset_total_files = 0
    for row in dataset_rows:
        try:
            file_count = int((row.get("file_count") or "0").strip())
        except Exception:
            file_count = 0
        dataset_total_files += file_count
        if file_count > 0:
            dataset_sets_with_files += 1

    generated_label = now.strftime("%b %d, %Y %H:%M UTC")
    window_label = f"{window_start.strftime('%b %d, %Y %H:%M UTC')} to {generated_label}"

    readme_lines = [
        README_START,
        "### What Changed in Last 24 Hours (Auto-generated)",
        f"- Window: {window_label}.",
        f"- Monitored link updates: {len(deduped_events)} across {len(domain_counts)} domains.",
        (
            "- Primary-doc diffs: "
            f"added {primary_counts.get('added', 0)}, "
            f"removed {primary_counts.get('removed', 0)}, "
            f"changed {primary_counts.get('changed', 0)}."
        ),
        (
            "- Claim-status diffs: "
            f"added {claim_counts.get('added', 0)}, "
            f"removed {claim_counts.get('removed', 0)}, "
            f"changed {claim_counts.get('changed', 0)}."
        ),
        f"- Claim review queue (open): p1={queue_counts.get('p1', 0)}, p2={queue_counts.get('p2', 0)}, p3={queue_counts.get('p3', 0)}.",
        (
            "- DOJ data-set file index: "
            f"{dataset_sets_with_files}/{dataset_total_sets} sets with files, "
            f"{dataset_total_files} total indexed files."
        ),
        "",
        "#### Recent items (latest first)",
    ]
    if not recent_events:
        readme_lines.append("- No monitored links in the current 24-hour window.")
    else:
        for event in recent_events:
            source = event["domain"] or event["source"] or "source"
            readme_lines.append(
                f"*   **{event['date']}**: {event['title']} ({source}). [Source]({event['url']})"
            )
    readme_lines.append(README_END)

    timeline_lines = [
        TIMELINE_START,
        "## Last 24 Hours Snapshot (Auto-generated)",
        f"- Window: {window_label}.",
        f"- Monitored updates: {len(deduped_events)} links across {len(domain_counts)} domains.",
        (
            "- Primary-doc changes: "
            f"added {primary_counts.get('added', 0)}, "
            f"removed {primary_counts.get('removed', 0)}, "
            f"changed {primary_counts.get('changed', 0)}."
        ),
        (
            "- Claim review pressure: "
            f"p1={queue_counts.get('p1', 0)}, p2={queue_counts.get('p2', 0)}, p3={queue_counts.get('p3', 0)} open."
        ),
        (
            "- DOJ data-set file index health: "
            f"{dataset_sets_with_files}/{dataset_total_sets} sets with files."
        ),
        "",
        "### Recent items",
    ]
    if not recent_events:
        timeline_lines.append("- No monitored links in the current 24-hour window.")
    else:
        for event in recent_events:
            source = event["domain"] or event["source"] or "source"
            timeline_lines.append(
                f"* **{event['date']}**: {event['title']} ({source}). [Source]({event['url']})"
            )
    timeline_lines.append(TIMELINE_END)

    readme_text = readme_path.read_text(encoding="utf-8")
    readme_path.write_text(
        replace_or_append_block(readme_text, README_START, README_END, "\n".join(readme_lines)) + "\n",
        encoding="utf-8",
    )

    timeline_text = timeline_path.read_text(encoding="utf-8")
    timeline_path.write_text(
        replace_or_append_block(timeline_text, TIMELINE_START, TIMELINE_END, "\n".join(timeline_lines)) + "\n",
        encoding="utf-8",
    )

    print("Last-24h brief updated.")
    print(f"- {readme_path}")
    print(f"- {timeline_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
