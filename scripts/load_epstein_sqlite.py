#!/usr/bin/env python3
"""Load normalized TSV outputs into the SQLite evidence database."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import pathlib
import sqlite3
import sys


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_id_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def normalize_name(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in value)
    return " ".join(cleaned.split())


def to_float(value: str) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def classify_entity_type(asserted_by: str) -> str:
    lower = asserted_by.lower()
    if any(token in lower for token in ("committee", "house", "senate", "congress")):
        return "committee"
    if any(token in lower for token in ("department", "doj", "fbi", "agency", "gov")):
        return "agency"
    return "organization"


def hash_id(prefix: str, seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Load evidence TSVs into SQLite.")
    parser.add_argument(
        "--db-path",
        default=str(root / "derived" / "database" / "epstein_research.sqlite"),
        help="SQLite database path.",
    )
    parser.add_argument(
        "--schema-path",
        default=str(root / "schema" / "epstein_research_schema.sql"),
        help="Schema SQL path.",
    )
    parser.add_argument(
        "--docs-tsv",
        default=str(root / "derived" / "primary_docs" / "primary_documents_latest.tsv"),
        help="Primary documents TSV path.",
    )
    parser.add_argument(
        "--claims-tsv",
        default=str(root / "derived" / "claims" / "claim_registry_latest.tsv"),
        help="Claim registry TSV path.",
    )
    parser.add_argument(
        "--links-tsv",
        default=str(root / "derived" / "claims" / "claim_evidence_links_latest.tsv"),
        help="Claim evidence TSV path.",
    )
    parser.add_argument(
        "--no-prune-missing-docs",
        action="store_true",
        help="Keep old pipeline documents not present in the current docs TSV.",
    )
    return parser.parse_args()


def ensure_placeholder_document(
    conn: sqlite3.Connection,
    doc_id: str,
    url: str,
    run_id: str,
    now_iso: str,
) -> None:
    conn.execute(
        """
        INSERT INTO documents (
          doc_id, source_system, document_type, jurisdiction, title,
          doc_date, url, citation, status, extracted_from, last_seen_run_id,
          created_at_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doc_id) DO UPDATE SET
          url = excluded.url,
          updated_at_utc = excluded.updated_at_utc,
          last_seen_run_id = excluded.last_seen_run_id
        """,
        (
            doc_id,
            "placeholder",
            "reference_stub",
            "",
            "Placeholder document for claim evidence link",
            "",
            url,
            "",
            "pending_resolution",
            "claims_loader",
            run_id,
            now_iso,
            now_iso,
        ),
    )


def main() -> int:
    args = parse_args()
    db_path = pathlib.Path(args.db_path).resolve()
    schema_path = pathlib.Path(args.schema_path).resolve()
    docs_tsv = pathlib.Path(args.docs_tsv).resolve()
    claims_tsv = pathlib.Path(args.claims_tsv).resolve()
    links_tsv = pathlib.Path(args.links_tsv).resolve()

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if not schema_path.exists():
        print(f"Schema not found: {schema_path}", file=sys.stderr)
        return 1

    documents = read_tsv(docs_tsv)
    claims = read_tsv(claims_tsv)
    links = read_tsv(links_tsv)

    run_id = run_id_now()
    now_iso = utc_now_iso()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    schema_sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(schema_sql)

    with conn:
        conn.execute(
            "INSERT INTO ingest_runs (run_id, ingested_at_utc, pipeline_name, notes) VALUES (?, ?, ?, ?)",
            (run_id, now_iso, "load_epstein_sqlite", "Load latest primary_docs and claims TSVs"),
        )

        existing_url_map = {row[0]: row[1] for row in conn.execute("SELECT url, doc_id FROM documents")}
        doc_id_redirects: dict[str, str] = {}
        current_doc_ids: set[str] = set()

        for row in documents:
            incoming_doc_id = (row.get("doc_id") or "").strip()
            url = (row.get("url") or "").strip()
            if not incoming_doc_id or not url:
                continue

            effective_doc_id = existing_url_map.get(url, incoming_doc_id)
            if effective_doc_id != incoming_doc_id:
                doc_id_redirects[incoming_doc_id] = effective_doc_id
            current_doc_ids.add(effective_doc_id)

            conn.execute(
                """
                INSERT INTO documents (
                  doc_id, source_system, document_type, jurisdiction, title,
                  doc_date, url, citation, status, extracted_from, last_seen_run_id,
                  created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                  source_system = excluded.source_system,
                  document_type = excluded.document_type,
                  jurisdiction = excluded.jurisdiction,
                  title = excluded.title,
                  doc_date = excluded.doc_date,
                  url = excluded.url,
                  citation = excluded.citation,
                  status = excluded.status,
                  extracted_from = excluded.extracted_from,
                  last_seen_run_id = excluded.last_seen_run_id,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    effective_doc_id,
                    (row.get("source_system") or "").strip(),
                    (row.get("document_type") or "").strip(),
                    (row.get("jurisdiction") or "").strip(),
                    (row.get("title") or "").strip(),
                    (row.get("doc_date") or "").strip(),
                    url,
                    (row.get("citation") or "").strip(),
                    (row.get("status") or "").strip(),
                    (row.get("extracted_from") or "").strip(),
                    run_id,
                    now_iso,
                    now_iso,
                ),
            )

        if not args.no_prune_missing_docs:
            managed_sources = (
                "courtlistener",
                "courtlistener_recap",
                "house_judiciary",
                "govtrack",
                "govinfo",
                "govinfo_wssearch",
                "justice_opa",
            )
            existing_managed = [
                row[0]
                for row in conn.execute(
                    f"SELECT doc_id FROM documents WHERE source_system IN ({','.join('?' for _ in managed_sources)})",
                    managed_sources,
                )
            ]
            obsolete_doc_ids = [doc_id for doc_id in existing_managed if doc_id not in current_doc_ids]
            if obsolete_doc_ids:
                placeholders = ",".join("?" for _ in obsolete_doc_ids)
                conn.execute(f"DELETE FROM claim_evidence_links WHERE doc_id IN ({placeholders})", obsolete_doc_ids)
                conn.execute(f"DELETE FROM events WHERE source_doc_id IN ({placeholders})", obsolete_doc_ids)
                conn.execute(f"DELETE FROM documents WHERE doc_id IN ({placeholders})", obsolete_doc_ids)

        asserted_by_to_entity: dict[str, str] = {}
        for row in claims:
            asserted_by = (row.get("asserted_by") or "").strip()
            if not asserted_by or asserted_by.lower() == "system":
                continue
            if asserted_by in asserted_by_to_entity:
                continue

            entity_id = hash_id("entity", f"{classify_entity_type(asserted_by)}|{normalize_name(asserted_by)}")
            asserted_by_to_entity[asserted_by] = entity_id
            conn.execute(
                """
                INSERT INTO entities (
                  entity_id, entity_type, canonical_name, normalized_name,
                  notes, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entity_id) DO UPDATE SET
                  canonical_name = excluded.canonical_name,
                  normalized_name = excluded.normalized_name,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    entity_id,
                    classify_entity_type(asserted_by),
                    asserted_by,
                    normalize_name(asserted_by),
                    "Auto-created from claim registry loader",
                    now_iso,
                    now_iso,
                ),
            )

        incoming_claim_ids: list[str] = []
        for row in claims:
            claim_id = (row.get("claim_id") or "").strip()
            if not claim_id:
                continue
            incoming_claim_ids.append(claim_id)
            asserted_by = (row.get("asserted_by") or "").strip()
            asserted_entity_id = asserted_by_to_entity.get(asserted_by)

            conn.execute(
                """
                INSERT INTO claims (
                  claim_id, claim_text, claim_type, asserted_by_entity_id,
                  first_seen_date, status, confidence, notes,
                  created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(claim_id) DO UPDATE SET
                  claim_text = excluded.claim_text,
                  claim_type = excluded.claim_type,
                  asserted_by_entity_id = excluded.asserted_by_entity_id,
                  first_seen_date = excluded.first_seen_date,
                  status = excluded.status,
                  confidence = excluded.confidence,
                  notes = excluded.notes,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    claim_id,
                    (row.get("claim_text") or "").strip(),
                    (row.get("claim_type") or "").strip(),
                    asserted_entity_id,
                    (row.get("first_seen_date") or "").strip(),
                    (row.get("status") or "").strip(),
                    to_float((row.get("confidence") or "").strip()),
                    (row.get("notes") or "").strip(),
                    now_iso,
                    now_iso,
                ),
            )

        if incoming_claim_ids:
            placeholders = ",".join("?" for _ in incoming_claim_ids)
            conn.execute(
                f"DELETE FROM claim_evidence_links WHERE claim_id IN ({placeholders})",
                incoming_claim_ids,
            )

        existing_docs = {row[0] for row in conn.execute("SELECT doc_id FROM documents")}
        for row in links:
            claim_id = (row.get("claim_id") or "").strip()
            raw_doc_id = (row.get("doc_id") or "").strip()
            evidence_url = (row.get("evidence_url") or "").strip()
            if not claim_id or not raw_doc_id:
                continue

            doc_id = doc_id_redirects.get(raw_doc_id, raw_doc_id)
            if doc_id not in existing_docs:
                ensure_placeholder_document(conn, doc_id, evidence_url, run_id, now_iso)
                existing_docs.add(doc_id)

            claim_evidence_id = hash_id(
                "claim-evidence",
                "|".join(
                    [
                        claim_id,
                        doc_id,
                        (row.get("evidence_type") or "").strip(),
                        (row.get("evidence_strength") or "").strip(),
                        evidence_url,
                    ]
                ),
            )

            conn.execute(
                """
                INSERT INTO claim_evidence_links (
                  claim_evidence_id, claim_id, doc_id, evidence_type,
                  evidence_strength, quote_excerpt, evidence_url, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(claim_evidence_id) DO UPDATE SET
                  evidence_type = excluded.evidence_type,
                  evidence_strength = excluded.evidence_strength,
                  quote_excerpt = excluded.quote_excerpt,
                  evidence_url = excluded.evidence_url
                """,
                (
                    claim_evidence_id,
                    claim_id,
                    doc_id,
                    (row.get("evidence_type") or "").strip(),
                    (row.get("evidence_strength") or "").strip(),
                    (row.get("quote_excerpt") or "").strip(),
                    evidence_url,
                    now_iso,
                ),
            )

        event_type_map = {
            "congressional_hearing": "hearing",
            "congressional_hearing_transcript": "hearing",
            "congressional_bill": "bill",
            "bill_action": "vote",
            "public_law": "law",
            "agency_release": "release",
            "congressional_record_transcript": "congressional_record",
        }

        for row in documents:
            doc_type = (row.get("document_type") or "").strip()
            event_type = event_type_map.get(doc_type)
            if not event_type:
                continue
            source_doc_id = doc_id_redirects.get((row.get("doc_id") or "").strip(), (row.get("doc_id") or "").strip())
            if not source_doc_id:
                continue
            event_id = hash_id("event", f"{event_type}|{source_doc_id}")
            conn.execute(
                """
                INSERT INTO events (
                  event_id, event_type, title, event_date, event_datetime_utc,
                  jurisdiction, summary, source_doc_id, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                  title = excluded.title,
                  event_date = excluded.event_date,
                  jurisdiction = excluded.jurisdiction,
                  summary = excluded.summary,
                  source_doc_id = excluded.source_doc_id,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    event_id,
                    event_type,
                    (row.get("title") or "").strip(),
                    (row.get("doc_date") or "").strip(),
                    "",
                    (row.get("jurisdiction") or "").strip(),
                    (row.get("citation") or "").strip(),
                    source_doc_id,
                    now_iso,
                    now_iso,
                ),
            )

    conn.close()

    print("SQLite load complete.")
    print(f"- Database: {db_path}")
    print(f"- Documents loaded: {len(documents)}")
    print(f"- Claims loaded: {len(claims)}")
    print(f"- Claim evidence links loaded: {len(links)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
