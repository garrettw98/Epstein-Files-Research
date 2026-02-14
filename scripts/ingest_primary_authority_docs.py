#!/usr/bin/env python3
"""Ingest primary authority docs (court + congressional + DOJ releases)."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import html
import json
import pathlib
import re
import sys
import urllib.parse
import urllib.request
from collections import Counter

USER_AGENT = "Mozilla/5.0 (EpsteinResearchPrimaryDocs/1.0)"
COURTLISTENER_API = "https://www.courtlistener.com/api/rest/v4/search/"
COURTLISTENER_QUERIES = [
    ("jeffrey_epstein", "Jeffrey Epstein"),
    ("ghislaine_maxwell", "Ghislaine Maxwell"),
]
COURTLISTENER_RECAP_QUERIES = [
    ("jeffrey_epstein_recap", "Jeffrey Epstein"),
    ("ghislaine_maxwell_recap", "Ghislaine Maxwell"),
]
HOUSE_HEARING_URL = "https://judiciary.house.gov/committee-activity/hearings/oversight-us-department-justice-5"
GOVTRACK_BILL_URL = "https://www.govtrack.us/congress/bills/119/hr4405"
GOVINFO_LAW_URL = "https://www.govinfo.gov/link/plaw/119/public/38"
GOVINFO_WSSEARCH_API = "https://www.govinfo.gov/wssearch/search"
GOVINFO_TRANSCRIPT_QUERIES = [
    ("crec_jeffrey_epstein", "\"Jeffrey Epstein\" collection:CREC"),
    ("crec_ghislaine_maxwell", "\"Ghislaine Maxwell\" collection:CREC"),
    ("chrg_jeffrey_epstein", "\"Jeffrey Epstein\" collection:CHRG"),
    ("chrg_ghislaine_maxwell", "\"Ghislaine Maxwell\" collection:CHRG"),
]
TARGET_PHRASES = ("jeffrey epstein", "ghislaine maxwell", "epstein files")
DOJ_RELEASE_URLS = [
    (
        "doc-doj-opa-first-phase",
        "2025-02-27",
        "Attorney General Pamela Bondi releases first phase of declassified Epstein files",
        "https://www.justice.gov/opa/pr/attorney-general-pamela-bondi-releases-first-phase-declassified-epstein-files",
    ),
    (
        "doc-doj-opa-35m-pages",
        "2026-01-30",
        "Department of Justice publishes 3.5 million responsive pages in compliance with Epstein files law",
        "https://www.justice.gov/opa/pr/department-justice-publishes-35-million-responsive-pages-compliance-epstein-files",
    ),
]


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ts_utc() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def fetch_text(url: str, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def post_json(url: str, payload: dict[str, object], timeout: int = 25) -> dict[str, object]:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def strip_tags(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def to_tsv(value: str) -> str:
    return (value or "").replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def doc_id(prefix: str, url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{digest}"


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Ingest primary authority docs.")
    parser.add_argument(
        "--raw-dir",
        default=str(root / "raw" / "primary_docs"),
        help="Raw output directory.",
    )
    parser.add_argument(
        "--derived-dir",
        default=str(root / "derived" / "primary_docs"),
        help="Derived output directory.",
    )
    parser.add_argument(
        "--court-results-per-query",
        type=int,
        default=40,
        help="CourtListener rows per query.",
    )
    parser.add_argument(
        "--recap-results-per-query",
        type=int,
        default=20,
        help="CourtListener RECAP rows per query.",
    )
    parser.add_argument(
        "--transcript-results-per-query",
        type=int,
        default=25,
        help="GovInfo transcript rows per query.",
    )
    return parser.parse_args()


def parse_house_hearing(html_text: str) -> dict[str, str]:
    title_match = re.search(r"<h1[^>]*><span>(.*?)</span></h1>", html_text, flags=re.IGNORECASE | re.DOTALL)
    date_match = re.search(r"<time[^>]+>([^<]+)</time>", html_text)
    location_match = re.search(
        r"Location<span[^>]*>:</span></div><div class=\"field__item\">([^<]+)</div>",
        html_text,
        flags=re.IGNORECASE,
    )
    witness_match = re.search(
        r"<strong><u>WITNESS</u></strong>.*?<li><span>(.*?)</span></li>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    video_match = re.search(r"https://www\.youtube\.com/embed/([A-Za-z0-9_-]+)", html_text)
    return {
        "title": strip_tags(title_match.group(1)) if title_match else "Oversight of the U.S. Department of Justice",
        "date_line": strip_tags(date_match.group(1)) if date_match else "",
        "location": strip_tags(location_match.group(1)) if location_match else "",
        "witness": strip_tags(witness_match.group(1)) if witness_match else "",
        "video_url": f"https://www.youtube.com/watch?v={video_match.group(1)}" if video_match else "",
    }


def parse_govtrack_actions(html_text: str) -> list[tuple[str, str]]:
    table_match = re.search(r"<table id=\"status-row-grid\".*?</table>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not table_match:
        return []
    rows = re.findall(r"<tr class=\"status-item.*?</tr>", table_match.group(0), flags=re.DOTALL)
    actions: list[tuple[str, str]] = []
    for row in rows:
        date_match = re.search(r"<span class=\"nowrap\">(.*?)</span>", row, flags=re.DOTALL)
        label_match = re.search(r"<div class=\"status-label\"[^>]*>(.*?)</div>", row, flags=re.DOTALL)
        date_value = strip_tags(date_match.group(1)) if date_match else ""
        label_value = strip_tags(label_match.group(1)) if label_match else ""
        if date_value or label_value:
            actions.append((date_value, label_value))
    return actions


def parse_title(html_text: str) -> str:
    match = re.search(r"<title>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    return strip_tags(match.group(1)) if match else ""


def parse_textual_date(raw: str) -> str:
    if not raw:
        return ""
    for month in (
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ):
        match = re.search(rf"{month}\s+\d{{1,2}},\s+\d{{4}}", raw)
        if not match:
            continue
        try:
            parsed = dt.datetime.strptime(match.group(0), "%B %d, %Y")
        except ValueError:
            continue
        return parsed.strftime("%Y-%m-%d")
    return ""


def is_target_relevant(*parts: str) -> bool:
    haystack = " ".join((part or "").lower() for part in parts)
    return any(phrase in haystack for phrase in TARGET_PHRASES)


def main() -> int:
    args = parse_args()
    run_utc = ts_utc()
    root = pathlib.Path(__file__).resolve().parents[1]
    raw_dir = pathlib.Path(args.raw_dir).resolve()
    derived_dir = pathlib.Path(args.derived_dir).resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    derived_dir.mkdir(parents=True, exist_ok=True)

    docs: list[dict[str, str]] = []
    raw_artifacts: list[pathlib.Path] = []
    warnings: list[str] = []

    # CourtListener ingest
    for key, query in COURTLISTENER_QUERIES:
        params = urllib.parse.urlencode(
            {
                "q": query,
                "order_by": "dateFiled desc",
                "page_size": str(args.court_results_per_query),
            }
        )
        url = f"{COURTLISTENER_API}?{params}"
        payload = fetch_text(url)
        raw_path = raw_dir / f"{run_utc}_courtlistener_{key}.json"
        raw_path.write_text(payload, encoding="utf-8")
        raw_artifacts.append(raw_path)

        data = json.loads(payload)
        for row in data.get("results", []):
            absolute = row.get("absolute_url", "")
            record_url = urllib.parse.urljoin("https://www.courtlistener.com", absolute) if absolute else ""
            if not record_url:
                continue
            title = to_tsv(str(row.get("caseName", ""))) or "Untitled court record"
            filed = to_tsv(str(row.get("dateFiled", "")))
            court = to_tsv(str(row.get("court", "")))
            docket = to_tsv(str(row.get("docketNumber", "")))
            docs.append(
                {
                    "doc_id": doc_id("doc-courtlistener", record_url),
                    "source_system": "courtlistener",
                    "document_type": "court_record",
                    "jurisdiction": court,
                    "title": title,
                    "doc_date": filed,
                    "url": record_url,
                    "citation": docket,
                    "entity_tags": "jeffrey_epstein,ghislaine_maxwell",
                    "status": to_tsv(str(row.get("status", ""))),
                    "extracted_from": f"courtlistener:{query}",
                }
            )

    # CourtListener RECAP/PACER ingest via search type=r.
    for key, query in COURTLISTENER_RECAP_QUERIES:
        params = urllib.parse.urlencode(
            {
                "q": query,
                "type": "r",
                "order_by": "dateFiled desc",
                "page_size": str(args.recap_results_per_query),
            }
        )
        url = f"{COURTLISTENER_API}?{params}"
        payload = fetch_text(url)
        raw_path = raw_dir / f"{run_utc}_courtlistener_{key}.json"
        raw_path.write_text(payload, encoding="utf-8")
        raw_artifacts.append(raw_path)

        data = json.loads(payload)
        for row in data.get("results", []):
            recap_rows = row.get("recap_documents") or []
            if not isinstance(recap_rows, list):
                continue
            case_name = to_tsv(str(row.get("caseName", "")))
            docket_number = to_tsv(str(row.get("docketNumber", "")))
            court = to_tsv(str(row.get("court", "")))
            for recap in recap_rows:
                absolute = to_tsv(str(recap.get("absolute_url", "")))
                recap_url = urllib.parse.urljoin("https://www.courtlistener.com", absolute) if absolute else ""
                if not recap_url:
                    continue
                short_desc = to_tsv(str(recap.get("short_description", "")))
                long_desc = to_tsv(str(recap.get("description", "")))
                snippet = to_tsv(str(recap.get("snippet", "")))
                if not is_target_relevant(case_name, short_desc, long_desc, snippet):
                    continue
                entry_num = to_tsv(str(recap.get("entry_number", "")))
                doc_number = to_tsv(str(recap.get("document_number", "")))
                doc_title = case_name or "RECAP docket document"
                if short_desc:
                    doc_title = f"{doc_title} - {short_desc}"
                filed = to_tsv(str(recap.get("entry_date_filed", ""))) or to_tsv(str(row.get("dateFiled", "")))
                docs.append(
                    {
                        "doc_id": doc_id("doc-recap", recap_url),
                        "source_system": "courtlistener_recap",
                        "document_type": "pacer_recap_document",
                        "jurisdiction": court,
                        "title": doc_title,
                        "doc_date": filed,
                        "url": recap_url,
                        "citation": f"{docket_number} entry {entry_num} doc {doc_number}".strip(),
                        "entity_tags": "jeffrey_epstein,ghislaine_maxwell,pacer,recap",
                        "status": "available" if recap.get("is_available") else "unavailable",
                        "extracted_from": f"courtlistener_recap:{query}",
                    }
                )

    # House Judiciary hearing page
    house_html = fetch_text(HOUSE_HEARING_URL)
    house_raw_path = raw_dir / f"{run_utc}_house_hearing.html"
    house_raw_path.write_text(house_html, encoding="utf-8")
    raw_artifacts.append(house_raw_path)
    house_meta = parse_house_hearing(house_html)
    docs.append(
        {
            "doc_id": "doc-house-hearing-oversight-doj-2026",
            "source_system": "house_judiciary",
            "document_type": "congressional_hearing",
            "jurisdiction": "US Congress",
            "title": house_meta["title"],
            "doc_date": "2026-02-11",
            "url": HOUSE_HEARING_URL,
            "citation": "House Judiciary Hearing",
            "entity_tags": "pam_bondi,doj,epstein_files",
            "status": "published",
            "extracted_from": "house_judiciary",
        }
    )
    if house_meta["video_url"]:
        docs.append(
            {
                "doc_id": doc_id("doc-house-hearing-video", house_meta["video_url"]),
                "source_system": "house_judiciary",
                "document_type": "hearing_video",
                "jurisdiction": "US Congress",
                "title": f"{house_meta['title']} (Video)",
                "doc_date": "2026-02-11",
                "url": house_meta["video_url"],
                "citation": "House Judiciary YouTube Stream",
                "entity_tags": "pam_bondi,doj,epstein_files",
                "status": "published",
                "extracted_from": "house_judiciary",
            }
        )

    # GovTrack bill and action history
    govtrack_html = fetch_text(GOVTRACK_BILL_URL)
    govtrack_raw_path = raw_dir / f"{run_utc}_govtrack_hr4405.html"
    govtrack_raw_path.write_text(govtrack_html, encoding="utf-8")
    raw_artifacts.append(govtrack_raw_path)
    bill_title = parse_title(govtrack_html) or "H.R. 4405 — Epstein Files Transparency Act"
    docs.append(
        {
            "doc_id": "doc-govtrack-hr4405",
            "source_system": "govtrack",
            "document_type": "congressional_bill",
            "jurisdiction": "US Congress",
            "title": bill_title,
            "doc_date": "2025-07-15",
            "url": GOVTRACK_BILL_URL,
            "citation": "H.R. 4405",
            "entity_tags": "epstein_files_transparency_act",
            "status": "enacted",
            "extracted_from": "govtrack",
        }
    )
    for idx, (action_date, action_label) in enumerate(parse_govtrack_actions(govtrack_html), start=1):
        if not action_label:
            continue
        action_url = f"{GOVTRACK_BILL_URL}#status-row-grid"
        docs.append(
            {
                "doc_id": doc_id(f"doc-govtrack-action-{idx:02d}", action_url + action_date + action_label),
                "source_system": "govtrack",
                "document_type": "bill_action",
                "jurisdiction": "US Congress",
                "title": action_label,
                "doc_date": action_date,
                "url": action_url,
                "citation": "H.R. 4405 action",
                "entity_tags": "epstein_files_transparency_act",
                "status": "recorded",
                "extracted_from": "govtrack_status_row",
            }
        )

    # Public law link
    docs.append(
        {
            "doc_id": "doc-govinfo-pl119-38",
            "source_system": "govinfo",
            "document_type": "public_law",
            "jurisdiction": "US Federal Law",
            "title": "Public Law 119-38",
            "doc_date": "2025-11-19",
            "url": GOVINFO_LAW_URL,
            "citation": "Pub. L. 119-38",
            "entity_tags": "epstein_files_transparency_act",
            "status": "published",
            "extracted_from": "govinfo",
        }
    )

    # GovInfo transcript/congressional hearing search.
    for key, query in GOVINFO_TRANSCRIPT_QUERIES:
        payload = {
            "query": query,
            "offset": 0,
            "pageSize": args.transcript_results_per_query,
        }
        try:
            data = post_json(GOVINFO_WSSEARCH_API, payload)
        except Exception as err:
            warnings.append(f"govinfo_query_failed:{key}:{err}")
            continue

        raw_path = raw_dir / f"{run_utc}_govinfo_{key}.json"
        raw_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        raw_artifacts.append(raw_path)

        results = data.get("resultSet", [])
        if not isinstance(results, list):
            continue
        for result in results:
            if not isinstance(result, dict):
                continue
            field_map = result.get("fieldMap", {})
            if not isinstance(field_map, dict):
                continue
            url = to_tsv(str(field_map.get("url", "")))
            if not url:
                continue
            collection = to_tsv(str(field_map.get("collectionCode", "")))
            line1 = to_tsv(str(result.get("line1", "")))
            line2 = to_tsv(str(result.get("line2", "")))
            teaser = to_tsv(str(field_map.get("teaser", "")))
            if not is_target_relevant(line1, line2, teaser):
                continue
            title = to_tsv(str(field_map.get("title", ""))) or line1 or "Congressional transcript record"
            if collection == "CHRG":
                doc_type = "congressional_hearing_transcript"
            elif collection == "CREC":
                doc_type = "congressional_record_transcript"
            else:
                doc_type = "congressional_transcript"
            docs.append(
                {
                    "doc_id": doc_id("doc-govinfo-transcript", url),
                    "source_system": "govinfo_wssearch",
                    "document_type": doc_type,
                    "jurisdiction": "US Congress",
                    "title": title,
                    "doc_date": parse_textual_date(line2),
                    "url": url,
                    "citation": line1,
                    "entity_tags": "jeffrey_epstein,ghislaine_maxwell,congressional_transcript",
                    "status": "published",
                    "extracted_from": f"govinfo_wssearch:{query}",
                }
            )

    # DOJ OPA releases
    for release_doc_id, doc_date, title, url in DOJ_RELEASE_URLS:
        docs.append(
            {
                "doc_id": release_doc_id,
                "source_system": "justice_opa",
                "document_type": "agency_release",
                "jurisdiction": "US DOJ",
                "title": title,
                "doc_date": doc_date,
                "url": url,
                "citation": "DOJ OPA",
                "entity_tags": "doj,epstein_files",
                "status": "published",
                "extracted_from": "justice_opa",
            }
        )

    # Deduplicate by URL while preserving first record.
    dedup: dict[str, dict[str, str]] = {}
    for row in docs:
        dedup.setdefault(row["url"], row)
    final_rows = sorted(dedup.values(), key=lambda row: (row.get("doc_date", ""), row["source_system"], row["url"]))

    latest_tsv = derived_dir / "primary_documents_latest.tsv"
    timestamp_tsv = derived_dir / f"primary_documents_{run_utc}.tsv"
    with latest_tsv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "ingested_at_utc",
                "doc_id",
                "source_system",
                "document_type",
                "jurisdiction",
                "title",
                "doc_date",
                "url",
                "citation",
                "entity_tags",
                "status",
                "extracted_from",
            ]
        )
        for row in final_rows:
            writer.writerow(
                [
                    run_utc,
                    to_tsv(row["doc_id"]),
                    to_tsv(row["source_system"]),
                    to_tsv(row["document_type"]),
                    to_tsv(row["jurisdiction"]),
                    to_tsv(row["title"]),
                    to_tsv(row["doc_date"]),
                    to_tsv(row["url"]),
                    to_tsv(row["citation"]),
                    to_tsv(row["entity_tags"]),
                    to_tsv(row["status"]),
                    to_tsv(row["extracted_from"]),
                ]
            )
    timestamp_tsv.write_text(latest_tsv.read_text(encoding="utf-8"), encoding="utf-8")

    summary_latest = derived_dir / "primary_documents_summary_latest.md"
    summary_timestamp = derived_dir / f"primary_documents_summary_{run_utc}.md"
    by_source = Counter(row["source_system"] for row in final_rows)
    by_type = Counter(row["document_type"] for row in final_rows)
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Primary Documents Ingest Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Document count: {len(final_rows)}\n")
        handle.write(
            "- Sources queried: courtlistener, courtlistener_recap, house_judiciary, govtrack, govinfo, govinfo_wssearch, justice_opa\n\n"
        )
        handle.write("## By Source System\n")
        for key, value in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
            handle.write(f"- {key}: {value}\n")
        handle.write("\n## By Document Type\n")
        for key, value in sorted(by_type.items(), key=lambda item: (-item[1], item[0])):
            handle.write(f"- {key}: {value}\n")
        handle.write("\n## Raw Artifacts\n")
        for artifact in sorted(raw_artifacts):
            handle.write(f"- {artifact.relative_to(root)}\n")
        if warnings:
            handle.write("\n## Warnings\n")
            for warning in warnings:
                handle.write(f"- {warning}\n")
    summary_timestamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")

    run_manifest = {
        "run_utc": run_utc,
        "document_count": len(final_rows),
        "court_results_per_query": args.court_results_per_query,
        "recap_results_per_query": args.recap_results_per_query,
        "transcript_results_per_query": args.transcript_results_per_query,
        "queries": [query for _, query in COURTLISTENER_QUERIES],
        "recap_queries": [query for _, query in COURTLISTENER_RECAP_QUERIES],
        "govinfo_transcript_queries": [query for _, query in GOVINFO_TRANSCRIPT_QUERIES],
        "source_counts": dict(sorted(by_source.items())),
        "warnings": warnings,
    }
    (raw_dir / "run_manifest_latest.json").write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")
    (raw_dir / f"run_manifest_{run_utc}.json").write_text(json.dumps(run_manifest, indent=2), encoding="utf-8")

    print("Ingest complete.")
    print(f"- {latest_tsv}")
    print(f"- {summary_latest}")
    print(f"- Documents: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
