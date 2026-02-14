#!/usr/bin/env python3
"""Derive topic tags from primary document ingest outputs."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
from collections import Counter, defaultdict

TOPIC_RULES = [
    {
        "topic_id": "transparency_legislation",
        "topic_label": "Transparency Legislation",
        "keywords": [
            "transparency act",
            "h.r. 4405",
            "public law 119-38",
            "bill action",
            "enacted",
        ],
    },
    {
        "topic_id": "doj_release_operations",
        "topic_label": "DOJ Release Operations",
        "keywords": [
            "department of justice",
            "attorney general",
            "declassified epstein files",
            "responsive pages",
            "doj opa",
        ],
    },
    {
        "topic_id": "congressional_oversight",
        "topic_label": "Congressional Oversight",
        "keywords": [
            "oversight",
            "house judiciary",
            "committee",
            "hearing",
            "witness",
            "bondi",
        ],
    },
    {
        "topic_id": "congressional_record_activity",
        "topic_label": "Congressional Record Activity",
        "keywords": [
            "congressional record",
            "executive session",
            "executive calendar",
            "senate resolution",
            "public bills and resolutions",
            "release the epstein files",
        ],
    },
    {
        "topic_id": "court_and_litigation",
        "topic_label": "Court and Litigation",
        "keywords": [
            "district court",
            "complaint",
            "docket",
            "v.",
            "recap",
            "foia",
            "judicial watch",
            "leopold",
        ],
    },
    {
        "topic_id": "victim_and_survivor_issues",
        "topic_label": "Victim and Survivor Issues",
        "keywords": [
            "victim",
            "survivor",
            "human trafficking",
            "sex trafficking",
            "trafficking",
        ],
    },
    {
        "topic_id": "clemency_and_pardon",
        "topic_label": "Clemency and Pardon",
        "keywords": [
            "clemency",
            "pardon",
            "sense of the senate",
        ],
    },
    {
        "topic_id": "unsealing_and_access",
        "topic_label": "Unsealing and Access",
        "keywords": [
            "unseal",
            "unredacted",
            "redaction",
            "release",
            "files",
        ],
    },
]


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def to_tsv(value: str) -> str:
    return (value or "").replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Derive topic tags from primary docs.")
    parser.add_argument(
        "--input-tsv",
        default=str(root / "derived" / "primary_docs" / "primary_documents_latest.tsv"),
        help="Input primary docs TSV.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(root / "derived" / "topics"),
        help="Output directory for topic artifacts.",
    )
    return parser.parse_args()


def match_topics(row: dict[str, str]) -> list[tuple[dict[str, object], list[str], float]]:
    haystack = " ".join(
        [
            row.get("title", ""),
            row.get("citation", ""),
            row.get("source_system", ""),
            row.get("document_type", ""),
            row.get("entity_tags", ""),
            row.get("extracted_from", ""),
        ]
    ).lower()

    matches: list[tuple[dict[str, object], list[str], float]] = []
    for rule in TOPIC_RULES:
        hit_terms = [kw for kw in rule["keywords"] if kw in haystack]
        if not hit_terms:
            continue
        confidence = min(0.55 + (0.08 * len(hit_terms)), 0.95)
        matches.append((rule, hit_terms, confidence))

    if matches:
        return matches

    if row.get("document_type") in {"court_record", "pacer_recap_document"}:
        fallback = {
            "topic_id": "court_and_litigation",
            "topic_label": "Court and Litigation",
        }
        return [(fallback, ["fallback:court"], 0.5)]

    fallback = {
        "topic_id": "unclassified_epstein_records",
        "topic_label": "Unclassified Epstein Records",
    }
    return [(fallback, ["fallback:generic"], 0.35)]


def main() -> int:
    args = parse_args()
    run_utc = utc_stamp()

    input_tsv = pathlib.Path(args.input_tsv).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = read_tsv(input_tsv)

    topic_rows: list[list[str]] = []
    topic_doc_counts: Counter[str] = Counter()
    topic_samples: dict[str, list[str]] = defaultdict(list)

    for row in rows:
        doc_id = row.get("doc_id", "")
        for rule, matched_terms, confidence in match_topics(row):
            topic_id = str(rule["topic_id"])
            topic_label = str(rule["topic_label"])
            topic_rows.append(
                [
                    run_utc,
                    to_tsv(doc_id),
                    to_tsv(topic_id),
                    to_tsv(topic_label),
                    f"{confidence:.2f}",
                    to_tsv(", ".join(matched_terms)),
                    to_tsv(row.get("source_system", "")),
                    to_tsv(row.get("document_type", "")),
                    to_tsv(row.get("doc_date", "")),
                    to_tsv(row.get("url", "")),
                    to_tsv(row.get("title", "")),
                ]
            )
            topic_doc_counts[topic_id] += 1
            if len(topic_samples[topic_id]) < 3:
                topic_samples[topic_id].append(to_tsv(row.get("title", "")))

    topic_index_latest = out_dir / "primary_doc_topic_index_latest.tsv"
    topic_index_stamp = out_dir / f"primary_doc_topic_index_{run_utc}.tsv"
    with topic_index_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "doc_id",
                "topic_id",
                "topic_label",
                "confidence",
                "matched_terms",
                "source_system",
                "document_type",
                "doc_date",
                "url",
                "title",
            ]
        )
        writer.writerows(topic_rows)
    topic_index_stamp.write_text(topic_index_latest.read_text(encoding="utf-8"), encoding="utf-8")

    catalog_latest = out_dir / "topic_catalog_latest.tsv"
    catalog_stamp = out_dir / f"topic_catalog_{run_utc}.tsv"
    with catalog_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["topic_id", "topic_label", "tagged_rows", "sample_titles"])
        labels = {rule["topic_id"]: rule["topic_label"] for rule in TOPIC_RULES}
        labels.setdefault("unclassified_epstein_records", "Unclassified Epstein Records")
        for topic_id, count in sorted(topic_doc_counts.items(), key=lambda item: (-item[1], item[0])):
            writer.writerow(
                [
                    topic_id,
                    labels.get(topic_id, topic_id),
                    str(count),
                    " | ".join(topic_samples.get(topic_id, [])),
                ]
            )
    catalog_stamp.write_text(catalog_latest.read_text(encoding="utf-8"), encoding="utf-8")

    summary_latest = out_dir / "primary_doc_topics_summary_latest.md"
    summary_stamp = out_dir / f"primary_doc_topics_summary_{run_utc}.md"
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Primary Document Topic Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Input documents: {len(rows)}\n")
        handle.write(f"- Tagged rows: {len(topic_rows)}\n")
        handle.write(f"- Unique topics: {len(topic_doc_counts)}\n\n")
        handle.write("## Topic Counts\n")
        labels = {rule["topic_id"]: rule["topic_label"] for rule in TOPIC_RULES}
        labels.setdefault("unclassified_epstein_records", "Unclassified Epstein Records")
        for topic_id, count in sorted(topic_doc_counts.items(), key=lambda item: (-item[1], item[0])):
            handle.write(f"- {labels.get(topic_id, topic_id)} ({topic_id}): {count}\n")

    summary_stamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")

    print("Primary doc topic derivation complete.")
    print(f"- {topic_index_latest}")
    print(f"- {catalog_latest}")
    print(f"- {summary_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
