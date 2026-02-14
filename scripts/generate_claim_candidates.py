#!/usr/bin/env python3
"""Generate pending-review claim candidates from primary document records."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import pathlib
from collections import Counter

TOPIC_HINTS = {
    "agency_release": "doj_release_operations",
    "congressional_hearing": "congressional_oversight",
    "hearing_video": "congressional_oversight",
    "congressional_bill": "transparency_legislation",
    "bill_action": "transparency_legislation",
    "public_law": "transparency_legislation",
    "congressional_record_transcript": "congressional_record_activity",
    "congressional_hearing_transcript": "congressional_oversight",
    "court_record": "court_and_litigation",
    "pacer_recap_document": "court_and_litigation",
}

PRIMARY_DOC_TYPES = {
    "agency_release",
    "congressional_hearing",
    "congressional_bill",
    "bill_action",
    "public_law",
    "congressional_record_transcript",
    "congressional_hearing_transcript",
    "court_record",
    "pacer_recap_document",
}


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def to_tsv(value: str) -> str:
    return (value or "").replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def candidate_id(seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"candidate-{digest}"


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Generate claim candidates from primary docs.")
    parser.add_argument(
        "--docs-tsv",
        default=str(root / "derived" / "primary_docs" / "primary_documents_latest.tsv"),
        help="Input primary docs TSV.",
    )
    parser.add_argument(
        "--topics-tsv",
        default=str(root / "derived" / "topics" / "primary_doc_topic_index_latest.tsv"),
        help="Optional topic index TSV for topic hints.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(root / "derived" / "claims"),
        help="Output directory for candidate claims.",
    )
    return parser.parse_args()


def best_topic_by_doc(topics_rows: list[dict[str, str]]) -> dict[str, str]:
    best: dict[str, tuple[float, str]] = {}
    for row in topics_rows:
        doc_id = row.get("doc_id", "")
        topic_id = row.get("topic_id", "")
        conf_raw = row.get("confidence", "")
        try:
            confidence = float(conf_raw)
        except Exception:
            confidence = 0.0
        current = best.get(doc_id)
        if current is None or confidence > current[0]:
            best[doc_id] = (confidence, topic_id)
    return {doc_id: topic for doc_id, (_, topic) in best.items()}


def is_epstein_core_title(title: str) -> bool:
    lower = title.lower()
    return any(term in lower for term in ("epstein", "ghislaine", "maxwell", "giuffre", "epstein files"))


def claim_from_row(row: dict[str, str]) -> tuple[str, str, str, float, str] | None:
    doc_type = row.get("document_type", "")
    title = to_tsv(row.get("title", ""))
    doc_date = to_tsv(row.get("doc_date", ""))
    citation = to_tsv(row.get("citation", ""))

    if doc_type == "agency_release":
        text = f"DOJ OPA published '{title}' on {doc_date}."
        return text, "procedural", "DOJ OPA", 0.80, "Primary agency release record"

    if doc_type == "congressional_hearing":
        text = f"House Judiciary hearing '{title}' was held on {doc_date}."
        return text, "timeline", "House Judiciary Committee", 0.82, "Primary hearing record"

    if doc_type == "congressional_bill":
        text = f"GovTrack records '{title}' with document date {doc_date}."
        return text, "legal", "GovTrack", 0.76, "Primary congressional bill record"

    if doc_type == "bill_action":
        text = f"GovTrack logs a bill action on {doc_date}: {title}."
        return text, "procedural", "GovTrack", 0.75, "Primary bill action history"

    if doc_type == "public_law":
        text = f"GovInfo provides a Public Law reference '{citation}' for Epstein transparency legislation."
        return text, "legal", "GovInfo", 0.83, "Primary public law link"

    if doc_type == "congressional_record_transcript":
        if not is_epstein_core_title(title):
            return None
        text = f"Congressional Record entry '{title}' appears with date {doc_date}."
        return text, "timeline", "US Congress", 0.70, "Primary congressional transcript"

    if doc_type == "congressional_hearing_transcript":
        if not is_epstein_core_title(title):
            return None
        text = f"Congressional hearing transcript '{title}' is available with date {doc_date}."
        return text, "timeline", "US Congress", 0.72, "Primary hearing transcript"

    if doc_type == "court_record":
        if not is_epstein_core_title(title):
            return None
        text = f"Court record '{title}' is indexed with filing date {doc_date}."
        return text, "procedural", "CourtListener", 0.68, "Primary court index record"

    if doc_type == "pacer_recap_document":
        if not is_epstein_core_title(title):
            return None
        text = f"RECAP docket document '{title}' is available with filing date {doc_date}."
        return text, "procedural", "CourtListener RECAP", 0.66, "Primary PACER/RECAP docket record"

    return None


def main() -> int:
    args = parse_args()
    run_utc = utc_stamp()

    docs_tsv = pathlib.Path(args.docs_tsv).resolve()
    topics_tsv = pathlib.Path(args.topics_tsv).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    docs = read_tsv(docs_tsv)
    topics = read_tsv(topics_tsv)
    topic_lookup = best_topic_by_doc(topics)

    generated: list[list[str]] = []
    seen_claim_texts: set[str] = set()

    for row in docs:
        doc_type = row.get("document_type", "")
        if doc_type not in PRIMARY_DOC_TYPES:
            continue

        generated_claim = claim_from_row(row)
        if generated_claim is None:
            continue

        claim_text, claim_type, asserted_by, confidence, rationale = generated_claim
        normalized_text = claim_text.lower()
        if normalized_text in seen_claim_texts:
            continue
        seen_claim_texts.add(normalized_text)

        doc_id = to_tsv(row.get("doc_id", ""))
        evidence_url = to_tsv(row.get("url", ""))
        topic_id = topic_lookup.get(doc_id, TOPIC_HINTS.get(doc_type, "unclassified_epstein_records"))
        candidate = [
            run_utc,
            candidate_id(f"{doc_id}|{claim_text}"),
            to_tsv(claim_text),
            claim_type,
            asserted_by,
            to_tsv(row.get("doc_date", "")),
            "pending_review",
            f"{confidence:.2f}",
            to_tsv(topic_id),
            doc_id,
            evidence_url,
            to_tsv(rationale),
        ]
        generated.append(candidate)

    latest_tsv = out_dir / "claim_candidates_latest.tsv"
    stamp_tsv = out_dir / f"claim_candidates_{run_utc}.tsv"
    with latest_tsv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "candidate_id",
                "claim_text",
                "claim_type",
                "asserted_by",
                "first_seen_date",
                "proposed_status",
                "confidence",
                "topic_id",
                "evidence_doc_id",
                "evidence_url",
                "rationale",
            ]
        )
        writer.writerows(generated)
    stamp_tsv.write_text(latest_tsv.read_text(encoding="utf-8"), encoding="utf-8")

    by_type = Counter(row[3] for row in generated)
    by_topic = Counter(row[8] for row in generated)

    summary_latest = out_dir / "claim_candidates_summary_latest.md"
    summary_stamp = out_dir / f"claim_candidates_summary_{run_utc}.md"
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Claim Candidate Generation Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Input docs: {len(docs)}\n")
        handle.write(f"- Generated candidates: {len(generated)}\n\n")

        handle.write("## By Claim Type\n")
        for key, value in sorted(by_type.items(), key=lambda item: (-item[1], item[0])):
            handle.write(f"- {key}: {value}\n")

        handle.write("\n## By Topic\n")
        for key, value in sorted(by_topic.items(), key=lambda item: (-item[1], item[0])):
            handle.write(f"- {key}: {value}\n")

    summary_stamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")

    print("Claim candidate generation complete.")
    print(f"- {latest_tsv}")
    print(f"- {summary_latest}")
    print(f"- Candidates: {len(generated)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
