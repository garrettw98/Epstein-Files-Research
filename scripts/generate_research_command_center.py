#!/usr/bin/env python3
"""Generate a one-page command center in Markdown and HTML."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import os
import pathlib
from collections import Counter


OK_HTTP_STATUSES = {"200", "301", "302"}
OPEN_TRIAGE_STATES = {"", "open", "in_review"}
OPEN_FLAG_STATES = {"", "open", "in_review", "new", "pending"}

NAV_TARGETS: list[tuple[str, str]] = [
    ("What changed now", "derived/reports/daily_change_report_latest.md"),
    ("Coverage health", "derived/reports/coverage_gap_dashboard_latest.md"),
    ("Evidence gaps", "derived/reports/primary_evidence_gap_register_latest.md"),
    ("Redaction patterns", "derived/reports/redaction_taxonomy_summary_latest.md"),
    ("Source authority index", "evidence/Primary_Sources_Index.md"),
    ("Timeline", "timeline/Full_Timeline.md"),
    ("Government response timeline", "timeline/Government_Response_To_Epstein_Files.md"),
    ("People index", "profiles/README.md"),
    ("Topic map", "topics/FAQ.md"),
    ("Core overview", "README.md"),
]


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_stamp() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def read_json(path: pathlib.Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def parse_compact_utc(value: str) -> dt.datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            parsed = dt.datetime.strptime(raw, fmt)
            return parsed.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def format_age_from_stamp(value: str, now: dt.datetime) -> str:
    parsed = parse_compact_utc(value)
    if not parsed:
        return "unknown age"
    return format_age_from_dt(parsed, now)


def format_age_from_dt(timestamp: dt.datetime, now: dt.datetime) -> str:
    delta = now - timestamp
    seconds = delta.total_seconds()
    if seconds < 0:
        return "in the future"
    if seconds < 3600:
        minutes = max(0, int(seconds // 60))
        return f"{minutes}m ago"
    hours = seconds / 3600.0
    if hours < 48:
        return f"{hours:.1f}h ago"
    return f"{(hours / 24.0):.1f}d ago"


def format_mtime_age(path: pathlib.Path, now: dt.datetime) -> str:
    if not path.exists():
        return "missing"
    modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    return format_age_from_dt(modified, now)


def to_int(value: str, default: int = 0) -> int:
    try:
        return int(float((value or "").strip()))
    except Exception:
        return default


def to_float(value: str, default: float = 0.0) -> float:
    try:
        return float((value or "").strip())
    except Exception:
        return default


def normalize_http_status(value: str) -> str:
    return (value or "").strip().split()[0] if (value or "").strip() else ""


def summarize_review_queue(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter({"p1": 0, "p2": 0, "p3": 0})
    for row in rows:
        triage_state = (row.get("triage_status") or "").strip().lower()
        if triage_state not in OPEN_TRIAGE_STATES:
            continue
        priority = (row.get("priority") or "").strip().lower()
        if priority in {"p1", "p2", "p3"}:
            counts[priority] += 1
    return counts


def summarize_quality_flags(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        flag_state = (row.get("flag_status") or "").strip().lower()
        if flag_state not in OPEN_FLAG_STATES:
            continue
        severity = (row.get("severity") or "").strip().lower() or "unknown"
        counts[severity] += 1
    return counts


def summarize_change_types(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        change_type = (row.get("change_type") or "").strip().lower()
        if change_type:
            counts[change_type] += 1
    return counts


def summarize_claim_status(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        status = (row.get("status") or "").strip().lower()
        if status:
            counts[status] += 1
    return counts


def aggregate_entity_mentions(rows: list[dict[str, str]], limit: int) -> list[tuple[str, float]]:
    counts: Counter[str] = Counter()
    for row in rows:
        canonical_name = (row.get("canonical_name") or "").strip()
        if not canonical_name:
            continue
        mention_count = to_float(row.get("mention_count") or "1", 1.0)
        if mention_count <= 0:
            mention_count = 1.0
        counts[canonical_name] += mention_count
    return counts.most_common(max(1, limit))


def aggregate_topics(rows: list[dict[str, str]], limit: int) -> list[tuple[str, int, str]]:
    topics: list[tuple[str, int, str]] = []
    for row in rows:
        label = (row.get("topic_label") or row.get("topic_id") or "").strip()
        if not label:
            continue
        topics.append(
            (
                label,
                to_int(row.get("tagged_rows") or "0"),
                (row.get("topic_id") or "").strip(),
            )
        )
    topics.sort(key=lambda item: (-item[1], item[0].lower()))
    return topics[: max(1, limit)]


def metric_rows_to_map(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    mapped: dict[str, dict[str, str]] = {}
    for row in rows:
        key = (row.get("metric") or "").strip()
        if key:
            mapped[key] = row
    return mapped


def metric_int(metrics: dict[str, dict[str, str]], key: str, default: int = 0) -> int:
    return to_int(metrics.get(key, {}).get("value", ""), default)


def collect_media_failures(rows: list[dict[str, str]], limit: int) -> list[tuple[str, str, str]]:
    failures: list[tuple[str, str, str]] = []
    for row in rows:
        status = normalize_http_status(row.get("http_status") or "")
        if not status or status in OK_HTTP_STATUSES:
            continue
        failures.append(
            (
                (row.get("outlet") or "unknown").strip(),
                status,
                (row.get("endpoint") or "").strip(),
            )
        )
    return failures[: max(1, limit)]


def first_nonempty_run_stamp(rows: list[dict[str, str]], keys: list[str]) -> str:
    for row in rows:
        for key in keys:
            value = (row.get(key) or "").strip()
            if value:
                return value
    return ""


def status_label(counter: Counter[str]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(counter.items(), key=lambda item: item[0])) or "none"


def relative_href(root: pathlib.Path, out_dir: pathlib.Path, repo_relative_path: str) -> str:
    target = (root / repo_relative_path).resolve()
    rel = os.path.relpath(target, out_dir)
    return rel.replace(os.sep, "/")


def tone_for_count(value: int, invert: bool = False) -> str:
    if invert:
        return "ok" if value > 0 else "warn"
    return "warn" if value > 0 else "ok"


def bar_width(value: int, total: int) -> float:
    if total <= 0 or value <= 0:
        return 0.0
    raw = (value / float(total)) * 100.0
    return max(6.0, min(100.0, raw))


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Generate research command-center markdown and html.")
    parser.add_argument("--root", default=str(root), help="Repository root.")
    parser.add_argument(
        "--out-dir",
        default=str(root / "derived" / "reports"),
        help="Output directory for command-center reports.",
    )
    parser.add_argument("--top-entities", type=int, default=8, help="How many top entities to list.")
    parser.add_argument("--top-topics", type=int, default=8, help="How many top topics to list.")
    parser.add_argument("--sample-changes", type=int, default=8, help="How many diff samples to list.")
    parser.add_argument("--sample-failures", type=int, default=8, help="How many endpoint failures to list.")
    return parser.parse_args()


def build_markdown(data: dict[str, object], now: dt.datetime, root: pathlib.Path) -> str:
    lines = [
        "# Research Command Center",
        "",
        f"- Generated UTC: {data['run_utc']}",
        "- Purpose: one page to see current changes, risks, and where to dive deeper.",
        "",
        "## Snapshot",
        "",
        "| Signal | Current value |",
        "| :--- | :--- |",
        f"| Primary documents indexed | {data['primary_docs_total']} |",
        f"| Claims tracked | {data['claims_total']} ({data['claim_status_label']}) |",
        f"| Claims lacking tier-1 evidence links | {data['evidence_gap_total']} ({data['evidence_gap_label']}) |",
        (
            "| Claim review queue (open) | "
            f"p1={data['queue_p1']}, p2={data['queue_p2']}, p3={data['queue_p3']} |"
        ),
        (
            "| Open claim-quality flags | "
            f"high={data['quality_high']}, warn={data['quality_warn']}, info={data['quality_info']} |"
        ),
        (
            "| DOJ dataset coverage | "
            f"{data['dataset_sets_with_files']}/{data['expected_dataset_max'] or 'unknown'} sets with files, "
            f"{data['dataset_total_files']} files indexed |"
        ),
        (
            "| Coverage warnings | "
            f"missing_datasets={data['missing_dataset_count']}, "
            f"media_endpoint_failures={data['coverage_media_failures']}, stale_inputs={data['stale_inputs']} |"
        ),
        "",
        "## Change Pulse",
        "",
        (
            "- Primary-doc diffs: "
            f"added={data['primary_added']}, removed={data['primary_removed']}, changed={data['primary_changed']}."
        ),
        (
            "- Claim-status diffs: "
            f"added={data['claim_added']}, removed={data['claim_removed']}, changed={data['claim_changed']}."
        ),
        "",
        "### Primary Doc Samples",
    ]

    sample_primary_changes: list[dict[str, str]] = data["sample_primary_changes"]  # type: ignore[assignment]
    sample_claim_changes: list[dict[str, str]] = data["sample_claim_changes"]  # type: ignore[assignment]
    warn_metrics: list[dict[str, str]] = data["warn_metrics"]  # type: ignore[assignment]
    media_failures: list[tuple[str, str, str]] = data["media_failures"]  # type: ignore[assignment]
    top_entities: list[tuple[str, float]] = data["top_entities"]  # type: ignore[assignment]
    top_topics: list[tuple[str, int, str]] = data["top_topics"]  # type: ignore[assignment]
    actions: list[str] = data["actions"]  # type: ignore[assignment]

    if not sample_primary_changes:
        lines.append("- No primary-doc diffs found in `daily_primary_doc_diff_latest.tsv`.")
    else:
        for row in sample_primary_changes:
            lines.append(
                f"- {row.get('change_type', 'changed')}: `{row.get('doc_id', '')}` ({row.get('source_system', 'unknown')})"
            )

    lines.extend(["", "### Claim Change Samples"])
    if not sample_claim_changes:
        lines.append("- No claim-status diffs found in `daily_claim_status_changes_latest.tsv`.")
    else:
        for row in sample_claim_changes:
            lines.append(
                f"- {row.get('change_type', 'changed')}: `{row.get('claim_id', '')}` "
                f"({row.get('previous_status', '')} -> {row.get('new_status', '')})"
            )

    lines.extend(["", "## Quality and Coverage Alerts", ""])
    if not warn_metrics and not media_failures:
        lines.append("- No active warnings in `coverage_gap_metrics_latest.tsv` or endpoint checks.")
    else:
        for row in warn_metrics:
            metric = row.get("metric", "")
            if metric == "media_endpoint_failures":
                value = str(data["coverage_media_failures"])
                detail = "Outlet endpoints currently non-200/301/302 from latest status file."
            else:
                value = row.get("value", "")
                detail = row.get("detail", "")
            lines.append(f"- `{metric}` = {value}. {detail}")
        if media_failures:
            lines.append("")
            lines.append("### Failing Endpoints")
            for outlet, status, endpoint in media_failures:
                lines.append(f"- {outlet}: {status} ({endpoint})")

    lines.extend(["", "## Active Entities and Topics", ""])
    if top_entities:
        lines.append("### Top Entities (by mention_count)")
        for name, count in top_entities:
            lines.append(f"- {name}: {count:g}")
    else:
        lines.append("- No entity mentions available in `entity_mentions_latest.tsv`.")

    lines.append("")
    if top_topics:
        lines.append("### Top Topics (by tagged_rows)")
        for label, count, topic_id in top_topics:
            suffix = f" ({topic_id})" if topic_id else ""
            lines.append(f"- {label}{suffix}: {count}")
    else:
        lines.append("- No topic catalog data available in `topic_catalog_latest.tsv`.")

    lines.extend(["", "## Focus Actions", ""])
    for idx, action in enumerate(actions, start=1):
        lines.append(f"{idx}. {action}")

    lines.extend(
        [
            "",
            "## Data Freshness",
            "",
            (
                f"- Primary ingest run: {data['primary_run_utc'] or 'unknown'} "
                f"({format_age_from_stamp(str(data['primary_run_utc']), now) if data['primary_run_utc'] else format_mtime_age(root / 'raw' / 'primary_docs' / 'run_manifest_latest.json', now)})."
            ),
            (
                f"- Universe ingest run: {data['universe_run_utc'] or 'unknown'} "
                f"({format_age_from_stamp(str(data['universe_run_utc']), now) if data['universe_run_utc'] else format_mtime_age(root / 'raw' / 'epstein_universe' / 'run_manifest_latest.json', now)})."
            ),
            (
                f"- Claim queue run: {data['queue_run_utc'] or 'not reported in file'} "
                f"({format_age_from_stamp(str(data['queue_run_utc']), now) if data['queue_run_utc'] else format_mtime_age(root / 'derived' / 'claims' / 'claim_review_queue_latest.tsv', now)})."
            ),
            (
                f"- Quality flags run: {data['quality_run_utc'] or 'not reported in file'} "
                f"({format_age_from_stamp(str(data['quality_run_utc']), now) if data['quality_run_utc'] else format_mtime_age(root / 'derived' / 'claims' / 'claim_quality_flags_latest.tsv', now)})."
            ),
            (
                f"- Dataset completeness run: {data['dataset_run_utc'] or 'unknown'} "
                f"({format_age_from_stamp(str(data['dataset_run_utc']), now) if data['dataset_run_utc'] else format_mtime_age(root / 'derived' / 'doj_epstein_library' / 'dataset_file_counts_latest.tsv', now)})."
            ),
            "",
            "## Navigation",
            "",
        ]
    )

    nav_links: list[tuple[str, str]] = data["nav_links"]  # type: ignore[assignment]
    for label, repo_path in nav_links:
        lines.append(f"- {label}: `{repo_path}`")

    lines.extend(
        [
            "",
            "## Taxonomy Snapshot",
            "",
            f"- Redaction categories: {data['redaction_label']}.",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def html_li(text: str) -> str:
    return f"<li>{html.escape(text)}</li>"


def html_metric_card(label: str, value: str, sub: str, tone: str = "neutral") -> str:
    return (
        "<article class='metric-card reveal'>"
        f"<p class='metric-label'>{html.escape(label)}</p>"
        f"<p class='metric-value tone-{html.escape(tone)}'>{html.escape(value)}</p>"
        f"<p class='metric-sub'>{html.escape(sub)}</p>"
        "</article>"
    )


def html_meter_row(label: str, value: int, total: int, tone: str) -> str:
    width = bar_width(value, total)
    return (
        "<div class='meter-row'>"
        f"<div class='meter-head'><span>{html.escape(label)}</span><strong>{value}</strong></div>"
        "<div class='meter-track'>"
        f"<div class='meter-fill meter-{html.escape(tone)}' style='width:{width:.1f}%'></div>"
        "</div>"
        "</div>"
    )


def build_html(data: dict[str, object], now: dt.datetime, root: pathlib.Path, out_dir: pathlib.Path) -> str:
    sample_primary_changes: list[dict[str, str]] = data["sample_primary_changes"]  # type: ignore[assignment]
    sample_claim_changes: list[dict[str, str]] = data["sample_claim_changes"]  # type: ignore[assignment]
    warn_metrics: list[dict[str, str]] = data["warn_metrics"]  # type: ignore[assignment]
    media_failures: list[tuple[str, str, str]] = data["media_failures"]  # type: ignore[assignment]
    top_entities: list[tuple[str, float]] = data["top_entities"]  # type: ignore[assignment]
    top_topics: list[tuple[str, int, str]] = data["top_topics"]  # type: ignore[assignment]
    actions: list[str] = data["actions"]  # type: ignore[assignment]
    nav_links: list[tuple[str, str]] = data["nav_links"]  # type: ignore[assignment]

    freshness_items = [
        (
            "Primary ingest run",
            str(data["primary_run_utc"] or "unknown"),
            format_age_from_stamp(str(data["primary_run_utc"]), now)
            if data["primary_run_utc"]
            else format_mtime_age(root / "raw" / "primary_docs" / "run_manifest_latest.json", now),
        ),
        (
            "Universe ingest run",
            str(data["universe_run_utc"] or "unknown"),
            format_age_from_stamp(str(data["universe_run_utc"]), now)
            if data["universe_run_utc"]
            else format_mtime_age(root / "raw" / "epstein_universe" / "run_manifest_latest.json", now),
        ),
        (
            "Claim queue run",
            str(data["queue_run_utc"] or "not reported in file"),
            format_age_from_stamp(str(data["queue_run_utc"]), now)
            if data["queue_run_utc"]
            else format_mtime_age(root / "derived" / "claims" / "claim_review_queue_latest.tsv", now),
        ),
        (
            "Quality flags run",
            str(data["quality_run_utc"] or "not reported in file"),
            format_age_from_stamp(str(data["quality_run_utc"]), now)
            if data["quality_run_utc"]
            else format_mtime_age(root / "derived" / "claims" / "claim_quality_flags_latest.tsv", now),
        ),
        (
            "Dataset completeness run",
            str(data["dataset_run_utc"] or "unknown"),
            format_age_from_stamp(str(data["dataset_run_utc"]), now)
            if data["dataset_run_utc"]
            else format_mtime_age(root / "derived" / "doj_epstein_library" / "dataset_file_counts_latest.tsv", now),
        ),
    ]

    total_queue = int(data["queue_p1"]) + int(data["queue_p2"]) + int(data["queue_p3"])  # type: ignore[arg-type]
    total_primary_changes = int(data["primary_added"]) + int(data["primary_removed"]) + int(data["primary_changed"])  # type: ignore[arg-type]
    total_claim_changes = int(data["claim_added"]) + int(data["claim_removed"]) + int(data["claim_changed"])  # type: ignore[arg-type]

    primary_list = "".join(
        html_li(f"{row.get('change_type', 'changed')}: {row.get('doc_id', '')} ({row.get('source_system', 'unknown')})")
        for row in sample_primary_changes
    )
    claim_list = "".join(
        html_li(
            f"{row.get('change_type', 'changed')}: {row.get('claim_id', '')} "
            f"({row.get('previous_status', '')} -> {row.get('new_status', '')})"
        )
        for row in sample_claim_changes
    )

    if not primary_list:
        primary_list = html_li("No primary-doc diffs found in daily_primary_doc_diff_latest.tsv.")
    if not claim_list:
        claim_list = html_li("No claim-status diffs found in daily_claim_status_changes_latest.tsv.")

    alert_items: list[str] = []
    if not warn_metrics and not media_failures:
        alert_items.append("No active warnings in coverage_gap_metrics_latest.tsv or endpoint checks.")
    else:
        for row in warn_metrics:
            metric = row.get("metric", "")
            if metric == "media_endpoint_failures":
                value = str(data["coverage_media_failures"])
                detail = "Outlet endpoints currently non-200/301/302 from latest status file."
            else:
                value = row.get("value", "")
                detail = row.get("detail", "")
            alert_items.append(f"{metric} = {value}. {detail}")
        for outlet, status, endpoint in media_failures:
            alert_items.append(f"{outlet}: {status} ({endpoint})")
    alerts_html = "".join(html_li(item) for item in alert_items)

    entities_html = "".join(html_li(f"{name}: {count:g}") for name, count in top_entities) or html_li(
        "No entity mentions available in entity_mentions_latest.tsv."
    )

    topics_html = "".join(
        html_li(f"{label}{f' ({topic_id})' if topic_id else ''}: {count}")
        for label, count, topic_id in top_topics
    ) or html_li("No topic catalog data available in topic_catalog_latest.tsv.")

    actions_html = "".join(f"<li>{html.escape(action)}</li>" for action in actions)

    freshness_html = "".join(
        (
            "<li>"
            f"<span>{html.escape(label)}</span>"
            f"<strong>{html.escape(stamp)}</strong>"
            f"<em>{html.escape(age)}</em>"
            "</li>"
        )
        for label, stamp, age in freshness_items
    )

    nav_html = "".join(
        (
            "<a class='nav-link' href='"
            + html.escape(relative_href(root, out_dir, repo_path), quote=True)
            + "'>"
            + html.escape(label)
            + "</a>"
        )
        for label, repo_path in nav_links
    )

    queue_meters = "".join(
        [
            html_meter_row("P1", int(data["queue_p1"]), total_queue, "high"),  # type: ignore[arg-type]
            html_meter_row("P2", int(data["queue_p2"]), total_queue, "warn"),  # type: ignore[arg-type]
            html_meter_row("P3", int(data["queue_p3"]), total_queue, "ok"),  # type: ignore[arg-type]
        ]
    )

    primary_change_meters = "".join(
        [
            html_meter_row("Added", int(data["primary_added"]), total_primary_changes, "ok"),  # type: ignore[arg-type]
            html_meter_row("Removed", int(data["primary_removed"]), total_primary_changes, "high"),  # type: ignore[arg-type]
            html_meter_row("Changed", int(data["primary_changed"]), total_primary_changes, "warn"),  # type: ignore[arg-type]
        ]
    )

    claim_change_meters = "".join(
        [
            html_meter_row("Added", int(data["claim_added"]), total_claim_changes, "ok"),  # type: ignore[arg-type]
            html_meter_row("Removed", int(data["claim_removed"]), total_claim_changes, "high"),  # type: ignore[arg-type]
            html_meter_row("Changed", int(data["claim_changed"]), total_claim_changes, "warn"),  # type: ignore[arg-type]
        ]
    )

    header_stamp = html.escape(str(data["run_utc"]))

    metric_cards = "".join(
        [
            html_metric_card("Primary docs indexed", str(data["primary_docs_total"]), "Latest primary snapshot rows."),
            html_metric_card(
                "Claims tracked",
                str(data["claims_total"]),
                str(data["claim_status_label"]),
            ),
            html_metric_card(
                "Tier-1 evidence gaps",
                str(data["evidence_gap_total"]),
                str(data["evidence_gap_label"]),
                tone_for_count(int(data["evidence_gap_total"])),
            ),
            html_metric_card(
                "Dataset files indexed",
                str(data["dataset_total_files"]),
                f"{data['dataset_sets_with_files']}/{data['expected_dataset_max'] or 'unknown'} sets with files",
            ),
            html_metric_card(
                "Media endpoint failures",
                str(data["coverage_media_failures"]),
                "Non-200/301/302 status codes.",
                tone_for_count(int(data["coverage_media_failures"])),
            ),
            html_metric_card(
                "Stale inputs",
                str(data["stale_inputs"]),
                "Feeds older than stale threshold.",
                tone_for_count(int(data["stale_inputs"])),
            ),
        ]
    )

    return f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Research Command Center</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=Source+Serif+4:opsz,wght@8..60,400;8..60,600&family=IBM+Plex+Mono:wght@400;600&display=swap');

    :root {{
      --bg: #f2efe8;
      --bg2: #dce7ef;
      --ink: #101923;
      --muted: #4e5d6e;
      --card: rgba(255, 255, 255, 0.82);
      --line: rgba(16, 25, 35, 0.12);
      --ok: #0f7d56;
      --warn: #c66a11;
      --high: #b32128;
      --neutral: #17365d;
      --mono: 'IBM Plex Mono', 'Courier New', monospace;
      --sans: 'Space Grotesk', 'Helvetica Neue', sans-serif;
      --serif: 'Source Serif 4', Georgia, serif;
      --shadow: 0 10px 30px rgba(16, 25, 35, 0.09);
      --radius: 16px;
    }}

    * {{ box-sizing: border-box; }}

    body {{
      margin: 0;
      color: var(--ink);
      font-family: var(--sans);
      background:
        radial-gradient(circle at 10% -5%, rgba(221, 82, 62, 0.17), transparent 44%),
        radial-gradient(circle at 95% 10%, rgba(23, 54, 93, 0.17), transparent 40%),
        linear-gradient(135deg, var(--bg), var(--bg2));
      min-height: 100vh;
    }}

    .grid-overlay {{
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image: linear-gradient(to right, rgba(16, 25, 35, 0.03) 1px, transparent 1px),
                        linear-gradient(to bottom, rgba(16, 25, 35, 0.03) 1px, transparent 1px);
      background-size: 34px 34px;
      mask-image: radial-gradient(circle at center, black 20%, transparent 90%);
    }}

    main {{
      width: min(1180px, 92vw);
      margin: 28px auto 42px;
      display: grid;
      gap: 18px;
      position: relative;
      z-index: 1;
    }}

    .panel {{
      background: var(--card);
      backdrop-filter: blur(6px);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 18px;
    }}

    .hero h1 {{
      margin: 0;
      font-family: var(--serif);
      font-size: clamp(1.7rem, 2.2vw + 0.9rem, 2.6rem);
      letter-spacing: 0.2px;
    }}

    .hero .meta {{
      margin-top: 8px;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      color: var(--muted);
      font-size: 0.95rem;
    }}

    .pill {{
      display: inline-flex;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 10px;
      background: rgba(255, 255, 255, 0.65);
      font-family: var(--mono);
      font-size: 0.78rem;
    }}

    .section-title {{
      margin: 0 0 12px;
      font-size: 1.06rem;
      letter-spacing: 0.2px;
    }}

    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}

    .metric-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: rgba(255, 255, 255, 0.75);
    }}

    .metric-label {{
      margin: 0;
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .metric-value {{
      margin: 8px 0 4px;
      font-size: 1.55rem;
      line-height: 1.1;
      font-family: var(--serif);
      word-break: break-word;
    }}

    .metric-sub {{
      margin: 0;
      color: var(--muted);
      font-size: 0.88rem;
      word-break: break-word;
    }}

    .tone-ok {{ color: var(--ok); }}
    .tone-warn {{ color: var(--warn); }}
    .tone-high {{ color: var(--high); }}
    .tone-neutral {{ color: var(--neutral); }}

    .two-col {{
      display: grid;
      grid-template-columns: 1.1fr 1fr;
      gap: 14px;
    }}

    .inner-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: rgba(255, 255, 255, 0.72);
    }}

    .inner-card h3 {{
      margin: 0 0 10px;
      font-size: 0.96rem;
    }}

    .meter-row {{ margin-bottom: 9px; }}

    .meter-head {{
      display: flex;
      justify-content: space-between;
      margin-bottom: 5px;
      font-size: 0.87rem;
    }}

    .meter-track {{
      width: 100%;
      height: 8px;
      background: rgba(16, 25, 35, 0.11);
      border-radius: 999px;
      overflow: hidden;
    }}

    .meter-fill {{
      height: 100%;
      border-radius: inherit;
      transition: width 500ms ease;
    }}

    .meter-ok {{ background: linear-gradient(90deg, #0f7d56, #1ea777); }}
    .meter-warn {{ background: linear-gradient(90deg, #bf5f08, #e3903f); }}
    .meter-high {{ background: linear-gradient(90deg, #9f2126, #d34e45); }}

    ul, ol {{
      margin: 0;
      padding-left: 20px;
      display: grid;
      gap: 6px;
    }}

    li {{ line-height: 1.33; }}

    .small {{
      color: var(--muted);
      font-size: 0.85rem;
    }}

    .link-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 10px;
    }}

    .nav-link {{
      text-decoration: none;
      color: var(--ink);
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      background: rgba(255, 255, 255, 0.74);
      transition: transform 180ms ease, border-color 180ms ease, box-shadow 180ms ease;
      font-size: 0.92rem;
    }}

    .nav-link:hover {{
      transform: translateY(-1px);
      border-color: rgba(23, 54, 93, 0.45);
      box-shadow: 0 8px 18px rgba(23, 54, 93, 0.15);
    }}

    .freshness {{
      list-style: none;
      padding: 0;
      margin: 0;
      display: grid;
      gap: 8px;
    }}

    .freshness li {{
      display: grid;
      grid-template-columns: 1.2fr auto auto;
      gap: 10px;
      border-bottom: 1px dashed var(--line);
      padding-bottom: 7px;
      align-items: center;
      font-size: 0.9rem;
    }}

    .freshness strong {{ font-family: var(--mono); font-size: 0.78rem; }}
    .freshness em {{ color: var(--muted); font-style: normal; font-size: 0.82rem; }}

    .reveal {{
      opacity: 0;
      transform: translateY(10px);
      animation: reveal 620ms cubic-bezier(.2,.9,.22,1) forwards;
    }}

    .reveal:nth-of-type(1) {{ animation-delay: 0.05s; }}
    .reveal:nth-of-type(2) {{ animation-delay: 0.11s; }}
    .reveal:nth-of-type(3) {{ animation-delay: 0.17s; }}
    .reveal:nth-of-type(4) {{ animation-delay: 0.23s; }}
    .reveal:nth-of-type(5) {{ animation-delay: 0.29s; }}
    .reveal:nth-of-type(6) {{ animation-delay: 0.35s; }}

    @keyframes reveal {{
      from {{ opacity: 0; transform: translateY(10px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    @media (max-width: 900px) {{
      .two-col {{ grid-template-columns: 1fr; }}
      .freshness li {{ grid-template-columns: 1fr; gap: 3px; }}
    }}
  </style>
</head>
<body>
  <div class='grid-overlay' aria-hidden='true'></div>
  <main>
    <section class='panel hero reveal'>
      <h1>Research Command Center</h1>
      <div class='meta'>
        <span>Generated UTC {header_stamp}</span>
        <span class='pill'>single-page status + navigation</span>
      </div>
    </section>

    <section class='panel reveal'>
      <h2 class='section-title'>Snapshot</h2>
      <div class='metrics'>
        {metric_cards}
      </div>
    </section>

    <section class='panel reveal'>
      <h2 class='section-title'>Change Pulse</h2>
      <div class='two-col'>
        <div class='inner-card'>
          <h3>Primary Doc Changes</h3>
          {primary_change_meters}
          <ul>{primary_list}</ul>
        </div>
        <div class='inner-card'>
          <h3>Claim Status Changes</h3>
          {claim_change_meters}
          <ul>{claim_list}</ul>
        </div>
      </div>
    </section>

    <section class='panel reveal'>
      <h2 class='section-title'>Review Pressure</h2>
      <div class='two-col'>
        <div class='inner-card'>
          <h3>Claim Review Queue</h3>
          {queue_meters}
        </div>
        <div class='inner-card'>
          <h3>Quality and Coverage Alerts</h3>
          <ul>{alerts_html}</ul>
        </div>
      </div>
    </section>

    <section class='panel reveal'>
      <h2 class='section-title'>Active Entities and Topics</h2>
      <div class='two-col'>
        <div class='inner-card'>
          <h3>Top Entities (mention_count)</h3>
          <ul>{entities_html}</ul>
        </div>
        <div class='inner-card'>
          <h3>Top Topics (tagged_rows)</h3>
          <ul>{topics_html}</ul>
        </div>
      </div>
      <p class='small'>Redaction categories: {html.escape(str(data['redaction_label']))}</p>
    </section>

    <section class='panel reveal'>
      <h2 class='section-title'>Focus Actions</h2>
      <ol>{actions_html}</ol>
    </section>

    <section class='panel reveal'>
      <h2 class='section-title'>Data Freshness</h2>
      <ul class='freshness'>{freshness_html}</ul>
    </section>

    <section class='panel reveal'>
      <h2 class='section-title'>Navigation</h2>
      <div class='link-grid'>{nav_html}</div>
    </section>
  </main>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    now = now_utc()
    run_utc = utc_stamp()

    root = pathlib.Path(args.root).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    primary_docs_rows = read_tsv(root / "derived" / "primary_docs" / "primary_documents_latest.tsv")
    claim_registry_rows = read_tsv(root / "derived" / "claims" / "claim_registry_latest.tsv")
    queue_rows = read_tsv(root / "derived" / "claims" / "claim_review_queue_latest.tsv")
    quality_rows = read_tsv(root / "derived" / "claims" / "claim_quality_flags_latest.tsv")
    primary_diff_rows = read_tsv(root / "derived" / "reports" / "daily_primary_doc_diff_latest.tsv")
    claim_diff_rows = read_tsv(root / "derived" / "reports" / "daily_claim_status_changes_latest.tsv")
    coverage_metric_rows = read_tsv(root / "derived" / "reports" / "coverage_gap_metrics_latest.tsv")
    media_status_rows = read_tsv(root / "derived" / "media_coverage" / "outlet_endpoint_status_latest.tsv")
    evidence_gap_rows = read_tsv(root / "derived" / "reports" / "primary_evidence_gap_register_latest.tsv")
    redaction_rows = read_tsv(root / "derived" / "reports" / "redaction_taxonomy_latest.tsv")
    entity_rows = read_tsv(root / "derived" / "entities" / "entity_mentions_latest.tsv")
    topic_rows = read_tsv(root / "derived" / "topics" / "topic_catalog_latest.tsv")
    dataset_count_rows = read_tsv(root / "derived" / "doj_epstein_library" / "dataset_file_counts_latest.tsv")

    primary_manifest = read_json(root / "raw" / "primary_docs" / "run_manifest_latest.json")
    universe_manifest = read_json(root / "raw" / "epstein_universe" / "run_manifest_latest.json")

    claim_status_counts = summarize_claim_status(claim_registry_rows)
    queue_counts = summarize_review_queue(queue_rows)
    quality_counts = summarize_quality_flags(quality_rows)
    primary_change_counts = summarize_change_types(primary_diff_rows)
    claim_change_counts = summarize_change_types(claim_diff_rows)

    evidence_gap_by_status = Counter(
        (row.get("claim_status") or "").strip().lower() for row in evidence_gap_rows if row.get("claim_status")
    )
    redaction_category_counts = Counter(
        (row.get("redaction_category") or "").strip().lower() for row in redaction_rows if row.get("redaction_category")
    )
    top_entities = aggregate_entity_mentions(entity_rows, args.top_entities)
    top_topics = aggregate_topics(topic_rows, args.top_topics)
    all_media_failures = collect_media_failures(media_status_rows, max(1, len(media_status_rows)))
    media_failures = all_media_failures[: max(1, args.sample_failures)]

    metrics = metric_rows_to_map(coverage_metric_rows)
    warn_metrics = [row for row in coverage_metric_rows if (row.get("status") or "").strip().lower() == "warn"]

    expected_dataset_max = metric_int(metrics, "expected_dataset_max")
    missing_dataset_count = metric_int(metrics, "missing_dataset_count")
    dataset_sets_with_files = metric_int(metrics, "dataset_sets_with_files")
    dataset_total_files = metric_int(metrics, "dataset_total_files_indexed")
    stale_inputs = metric_int(metrics, "stale_inputs")
    coverage_media_failures = len(all_media_failures)

    primary_run_utc = str(primary_manifest.get("run_utc") or "").strip()
    universe_run_utc = str(universe_manifest.get("run_utc") or "").strip()
    queue_run_utc = first_nonempty_run_stamp(queue_rows, ["run_utc"])
    quality_run_utc = first_nonempty_run_stamp(quality_rows, ["run_utc"])
    dataset_run_utc = first_nonempty_run_stamp(dataset_count_rows, ["run_utc"])

    sample_primary_changes = primary_diff_rows[: max(1, args.sample_changes)]
    sample_claim_changes = claim_diff_rows[: max(1, args.sample_changes)]

    actions: list[str] = []
    if queue_counts.get("p1", 0) > 0:
        actions.append(
            f"Resolve {queue_counts.get('p1', 0)} p1 item(s) in derived/claims/claim_review_queue_latest.tsv."
        )
    if len(evidence_gap_rows) > 0:
        actions.append(
            f"Backfill tier-1 links for {len(evidence_gap_rows)} claim(s) in derived/reports/primary_evidence_gap_register_latest.tsv."
        )
    if missing_dataset_count > 0:
        actions.append("Investigate missing DOJ dataset coverage in derived/reports/coverage_gap_dashboard_latest.md.")
    if stale_inputs > 0:
        actions.append("Refresh stale ingest artifacts by running make daily-pipeline.")
    if coverage_media_failures > 0:
        actions.append("Review non-200 media endpoints in derived/media_coverage/outlet_endpoint_status_latest.tsv.")
    if not actions:
        actions.append("No blocking alerts detected; continue normal monitoring cadence.")

    data: dict[str, object] = {
        "run_utc": run_utc,
        "primary_docs_total": len(primary_docs_rows),
        "claims_total": len(claim_registry_rows),
        "evidence_gap_total": len(evidence_gap_rows),
        "claim_status_label": status_label(claim_status_counts),
        "evidence_gap_label": status_label(evidence_gap_by_status),
        "redaction_label": status_label(redaction_category_counts),
        "queue_p1": queue_counts.get("p1", 0),
        "queue_p2": queue_counts.get("p2", 0),
        "queue_p3": queue_counts.get("p3", 0),
        "quality_high": quality_counts.get("high", 0),
        "quality_warn": quality_counts.get("warn", 0),
        "quality_info": quality_counts.get("info", 0),
        "expected_dataset_max": expected_dataset_max,
        "missing_dataset_count": missing_dataset_count,
        "dataset_sets_with_files": dataset_sets_with_files,
        "dataset_total_files": dataset_total_files,
        "stale_inputs": stale_inputs,
        "coverage_media_failures": coverage_media_failures,
        "primary_added": primary_change_counts.get("added", 0),
        "primary_removed": primary_change_counts.get("removed", 0),
        "primary_changed": primary_change_counts.get("changed", 0),
        "claim_added": claim_change_counts.get("added", 0),
        "claim_removed": claim_change_counts.get("removed", 0),
        "claim_changed": claim_change_counts.get("changed", 0),
        "primary_run_utc": primary_run_utc,
        "universe_run_utc": universe_run_utc,
        "queue_run_utc": queue_run_utc,
        "quality_run_utc": quality_run_utc,
        "dataset_run_utc": dataset_run_utc,
        "sample_primary_changes": sample_primary_changes,
        "sample_claim_changes": sample_claim_changes,
        "warn_metrics": warn_metrics,
        "media_failures": media_failures,
        "top_entities": top_entities,
        "top_topics": top_topics,
        "actions": actions,
        "nav_links": NAV_TARGETS,
    }

    markdown_content = build_markdown(data, now, root)
    html_content = build_html(data, now, root, out_dir)

    latest_md = out_dir / "research_command_center_latest.md"
    stamped_md = out_dir / f"research_command_center_{run_utc}.md"
    latest_html = out_dir / "research_command_center_latest.html"
    stamped_html = out_dir / f"research_command_center_{run_utc}.html"

    latest_md.write_text(markdown_content, encoding="utf-8")
    stamped_md.write_text(markdown_content, encoding="utf-8")
    latest_html.write_text(html_content, encoding="utf-8")
    stamped_html.write_text(html_content, encoding="utf-8")

    print("Research command center generated.")
    print(f"- {latest_md}")
    print(f"- {stamped_md}")
    print(f"- {latest_html}")
    print(f"- {stamped_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
