#!/usr/bin/env python3
"""Load normalized TSV outputs into the SQLite evidence database."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import pathlib
import re
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


def sha1_text(value: str) -> str:
    return hashlib.sha1((value or "").encode("utf-8")).hexdigest()


def infer_source_tier(source_system: str) -> str:
    if source_system in {
        "govinfo",
        "govinfo_wssearch",
        "govtrack",
        "house_judiciary",
        "justice_opa",
        "courtlistener",
        "courtlistener_recap",
    }:
        return "tier1_primary"
    return "tier2_secondary"


def infer_capture_method(source_system: str, extracted_from: str) -> str:
    lowered = (extracted_from or "").lower()
    if source_system in {"courtlistener", "courtlistener_recap", "govinfo_wssearch"}:
        return "api"
    if source_system in {"house_judiciary", "govtrack"}:
        return "html_scrape"
    if source_system == "govinfo":
        return "direct_link"
    if source_system == "justice_opa":
        return "manual"
    if lowered.startswith("govtrack"):
        return "html_scrape"
    return "manual"


def infer_locator_type(locator: str) -> str:
    lowered = (locator or "").lower()
    if re.search(r"\bpage\b|\bp\.\s*\d+", lowered):
        return "page"
    if "line" in lowered:
        return "line"
    if "section" in lowered or "sec." in lowered:
        return "section"
    if re.search(r"\b\d{1,2}:\d{2}(:\d{2})?\b", lowered):
        return "timestamp"
    return "unknown"


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, ddl: str) -> None:
    if not table_exists(conn, table_name):
        return
    if column_name in table_columns(conn, table_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {ddl}")


def apply_runtime_migrations(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "documents", "source_tier", "source_tier TEXT NOT NULL DEFAULT 'tier2_secondary'")
    ensure_column(conn, "documents", "capture_method", "capture_method TEXT NOT NULL DEFAULT 'manual'")
    ensure_column(conn, "documents", "content_checksum", "content_checksum TEXT")
    ensure_column(conn, "documents", "first_seen_run_id", "first_seen_run_id TEXT")
    ensure_column(conn, "documents", "first_seen_at_utc", "first_seen_at_utc TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "documents", "last_seen_at_utc", "last_seen_at_utc TEXT NOT NULL DEFAULT ''")

    ensure_column(conn, "claim_evidence_links", "evidence_locator", "evidence_locator TEXT")
    ensure_column(conn, "claim_evidence_links", "snippet_hash", "snippet_hash TEXT")
    ensure_column(conn, "claim_evidence_links", "span_id", "span_id TEXT")
    ensure_column(conn, "claim_evidence_links", "provenance_note", "provenance_note TEXT")


def normalize_claim_status(raw_status: str, evidence_rows: list[dict[str, str]]) -> str:
    status = (raw_status or "").strip().lower()
    if status in {
        "verified_primary",
        "verified_secondary",
        "alleged",
        "disputed",
        "retracted",
        "pending_review",
    }:
        return status

    if status == "verified":
        primary_types = {"primary", "transcript", "filing", "release"}
        for row in evidence_rows:
            evidence_type = (row.get("evidence_type") or "").strip().lower()
            evidence_strength = (row.get("evidence_strength") or "").strip().lower()
            if evidence_type in primary_types and evidence_strength in {"direct", "supporting"}:
                return "verified_primary"
        return "verified_secondary"

    if status == "unverified":
        return "alleged"

    if status in {"disputed", "retracted", "pending_review"}:
        return status

    return "pending_review"


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
        "--candidates-tsv",
        default=str(root / "derived" / "claims" / "claim_candidates_latest.tsv"),
        help="Claim candidates TSV path.",
    )
    parser.add_argument(
        "--entity-aliases-tsv",
        default=str(root / "derived" / "entities" / "entity_aliases_resolved_latest.tsv"),
        help="Resolved entity aliases TSV path.",
    )
    parser.add_argument(
        "--entity-mentions-tsv",
        default=str(root / "derived" / "entities" / "entity_mentions_latest.tsv"),
        help="Entity mentions TSV path.",
    )
    parser.add_argument(
        "--claim-flags-tsv",
        default=str(root / "derived" / "claims" / "claim_quality_flags_latest.tsv"),
        help="Claim quality flags TSV path.",
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
    checksum_seed = "|".join([doc_id, url, "placeholder"])
    conn.execute(
        """
        INSERT INTO documents (
          doc_id, source_system, source_tier, capture_method, content_checksum,
          document_type, jurisdiction, title, doc_date, url, citation, status,
          extracted_from, first_seen_run_id, last_seen_run_id, first_seen_at_utc,
          last_seen_at_utc, created_at_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(doc_id) DO UPDATE SET
          url = excluded.url,
          source_tier = excluded.source_tier,
          capture_method = excluded.capture_method,
          content_checksum = excluded.content_checksum,
          last_seen_at_utc = excluded.last_seen_at_utc,
          updated_at_utc = excluded.updated_at_utc,
          last_seen_run_id = excluded.last_seen_run_id
        """,
        (
            doc_id,
            "placeholder",
            "tier3_aggregate",
            "manual",
            sha1_text(checksum_seed),
            "reference_stub",
            "",
            "Placeholder document for claim evidence link",
            "",
            url,
            "",
            "pending_resolution",
            "claims_loader",
            run_id,
            run_id,
            now_iso,
            now_iso,
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
    candidates_tsv = pathlib.Path(args.candidates_tsv).resolve()
    entity_aliases_tsv = pathlib.Path(args.entity_aliases_tsv).resolve()
    entity_mentions_tsv = pathlib.Path(args.entity_mentions_tsv).resolve()
    claim_flags_tsv = pathlib.Path(args.claim_flags_tsv).resolve()

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if not schema_path.exists():
        print(f"Schema not found: {schema_path}", file=sys.stderr)
        return 1

    documents = read_tsv(docs_tsv)
    claims = read_tsv(claims_tsv)
    links = read_tsv(links_tsv)
    candidates = read_tsv(candidates_tsv)
    entity_alias_rows = read_tsv(entity_aliases_tsv)
    entity_mention_rows = read_tsv(entity_mentions_tsv)
    claim_flag_rows = read_tsv(claim_flags_tsv)

    run_id = run_id_now()
    now_iso = utc_now_iso()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    # Migrate legacy tables before applying the latest DDL so new indexes do not fail.
    apply_runtime_migrations(conn)
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    apply_runtime_migrations(conn)

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
            source_system = (row.get("source_system") or "").strip()
            extracted_from = (row.get("extracted_from") or "").strip()
            source_tier = (row.get("source_tier") or "").strip() or infer_source_tier(source_system)
            capture_method = (row.get("capture_method") or "").strip() or infer_capture_method(source_system, extracted_from)
            content_checksum = (row.get("content_checksum") or "").strip()
            if not content_checksum:
                content_checksum = sha1_text(
                    "|".join(
                        [
                            url,
                            (row.get("title") or "").strip(),
                            (row.get("citation") or "").strip(),
                            (row.get("doc_date") or "").strip(),
                            (row.get("status") or "").strip(),
                        ]
                    )
                )

            conn.execute(
                """
                INSERT INTO documents (
                  doc_id, source_system, source_tier, capture_method, content_checksum,
                  document_type, jurisdiction, title, doc_date, url, citation, status,
                  extracted_from, first_seen_run_id, last_seen_run_id, first_seen_at_utc,
                  last_seen_at_utc, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                  source_system = excluded.source_system,
                  source_tier = excluded.source_tier,
                  capture_method = excluded.capture_method,
                  content_checksum = excluded.content_checksum,
                  document_type = excluded.document_type,
                  jurisdiction = excluded.jurisdiction,
                  title = excluded.title,
                  doc_date = excluded.doc_date,
                  url = excluded.url,
                  citation = excluded.citation,
                  status = excluded.status,
                  extracted_from = excluded.extracted_from,
                  last_seen_run_id = excluded.last_seen_run_id,
                  first_seen_run_id = CASE
                    WHEN documents.first_seen_run_id IS NULL OR documents.first_seen_run_id = ''
                    THEN excluded.first_seen_run_id
                    ELSE documents.first_seen_run_id
                  END,
                  first_seen_at_utc = CASE
                    WHEN documents.first_seen_at_utc IS NULL OR documents.first_seen_at_utc = ''
                    THEN excluded.first_seen_at_utc
                    ELSE documents.first_seen_at_utc
                  END,
                  last_seen_at_utc = excluded.last_seen_at_utc,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    effective_doc_id,
                    source_system,
                    source_tier,
                    capture_method,
                    content_checksum,
                    (row.get("document_type") or "").strip(),
                    (row.get("jurisdiction") or "").strip(),
                    (row.get("title") or "").strip(),
                    (row.get("doc_date") or "").strip(),
                    url,
                    (row.get("citation") or "").strip(),
                    (row.get("status") or "").strip(),
                    extracted_from,
                    run_id,
                    run_id,
                    now_iso,
                    now_iso,
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
                # Claim candidates can still reference old managed docs until the backlog refresh later in the run.
                # Remove those rows first so document pruning does not trip FK checks.
                conn.execute(f"DELETE FROM claim_candidates WHERE evidence_doc_id IN ({placeholders})", obsolete_doc_ids)
                conn.execute(f"DELETE FROM entity_mentions WHERE doc_id IN ({placeholders})", obsolete_doc_ids)
                conn.execute(f"DELETE FROM evidence_spans WHERE doc_id IN ({placeholders})", obsolete_doc_ids)
                conn.execute(f"DELETE FROM claim_quality_flags WHERE related_doc_id IN ({placeholders})", obsolete_doc_ids)
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

        links_by_claim: dict[str, list[dict[str, str]]] = {}
        for row in links:
            claim_key = (row.get("claim_id") or "").strip()
            if not claim_key:
                continue
            links_by_claim.setdefault(claim_key, []).append(row)

        incoming_claim_ids: list[str] = []
        for row in claims:
            claim_id = (row.get("claim_id") or "").strip()
            if not claim_id:
                continue
            incoming_claim_ids.append(claim_id)
            asserted_by = (row.get("asserted_by") or "").strip()
            asserted_entity_id = asserted_by_to_entity.get(asserted_by)
            normalized_status = normalize_claim_status((row.get("status") or "").strip(), links_by_claim.get(claim_id, []))

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
                    normalized_status,
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
            conn.execute(
                f"DELETE FROM claim_quality_flags WHERE claim_id IN ({placeholders})",
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

            evidence_type = (row.get("evidence_type") or "").strip()
            evidence_strength = (row.get("evidence_strength") or "").strip()
            quote_excerpt = (row.get("quote_excerpt") or "").strip()
            evidence_locator = (row.get("evidence_locator") or "").strip()
            provenance_note = (row.get("provenance_note") or "").strip()
            snippet_hash = (row.get("snippet_hash") or "").strip()
            if not snippet_hash and quote_excerpt:
                snippet_hash = sha1_text(quote_excerpt)

            span_id = (row.get("span_id") or "").strip()
            if not span_id and (evidence_locator or quote_excerpt):
                span_id = hash_id(
                    "span",
                    "|".join([doc_id, evidence_locator, snippet_hash, evidence_url]),
                )
            if span_id:
                conn.execute(
                    """
                    INSERT INTO evidence_spans (
                      span_id, doc_id, locator_type, locator_value, snippet_text,
                      snippet_hash, source_url, created_at_utc, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(span_id) DO UPDATE SET
                      locator_type = excluded.locator_type,
                      locator_value = excluded.locator_value,
                      snippet_text = excluded.snippet_text,
                      snippet_hash = excluded.snippet_hash,
                      source_url = excluded.source_url,
                      updated_at_utc = excluded.updated_at_utc
                    """,
                    (
                        span_id,
                        doc_id,
                        infer_locator_type(evidence_locator),
                        evidence_locator or "unknown",
                        quote_excerpt,
                        snippet_hash,
                        evidence_url,
                        now_iso,
                        now_iso,
                    ),
                )

            claim_evidence_id = hash_id(
                "claim-evidence",
                "|".join(
                    [
                        claim_id,
                        doc_id,
                        evidence_type,
                        evidence_strength,
                        evidence_url,
                        span_id,
                    ]
                ),
            )

            conn.execute(
                """
                INSERT INTO claim_evidence_links (
                  claim_evidence_id, claim_id, doc_id, evidence_type,
                  evidence_strength, evidence_locator, quote_excerpt, snippet_hash,
                  span_id, evidence_url, provenance_note, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(claim_evidence_id) DO UPDATE SET
                  evidence_type = excluded.evidence_type,
                  evidence_strength = excluded.evidence_strength,
                  evidence_locator = excluded.evidence_locator,
                  quote_excerpt = excluded.quote_excerpt,
                  snippet_hash = excluded.snippet_hash,
                  span_id = excluded.span_id,
                  evidence_url = excluded.evidence_url,
                  provenance_note = excluded.provenance_note
                """,
                (
                    claim_evidence_id,
                    claim_id,
                    doc_id,
                    evidence_type,
                    evidence_strength,
                    evidence_locator,
                    quote_excerpt,
                    snippet_hash,
                    span_id,
                    evidence_url,
                    provenance_note,
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

        # Replace candidate backlog on each load with current generated set.
        conn.execute("DELETE FROM claim_candidates")
        for row in candidates:
            candidate_id = (row.get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            evidence_doc_id = (row.get("evidence_doc_id") or "").strip()
            if evidence_doc_id and evidence_doc_id not in existing_docs:
                ensure_placeholder_document(
                    conn=conn,
                    doc_id=evidence_doc_id,
                    url=(row.get("evidence_url") or "").strip(),
                    run_id=run_id,
                    now_iso=now_iso,
                )
                existing_docs.add(evidence_doc_id)

            conn.execute(
                """
                INSERT INTO claim_candidates (
                  candidate_id, claim_text, claim_type, asserted_by, first_seen_date,
                  proposed_status, confidence, topic_id, evidence_doc_id, evidence_url,
                  rationale, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(candidate_id) DO UPDATE SET
                  claim_text = excluded.claim_text,
                  claim_type = excluded.claim_type,
                  asserted_by = excluded.asserted_by,
                  first_seen_date = excluded.first_seen_date,
                  proposed_status = excluded.proposed_status,
                  confidence = excluded.confidence,
                  topic_id = excluded.topic_id,
                  evidence_doc_id = excluded.evidence_doc_id,
                  evidence_url = excluded.evidence_url,
                  rationale = excluded.rationale,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    candidate_id,
                    (row.get("claim_text") or "").strip(),
                    (row.get("claim_type") or "").strip(),
                    (row.get("asserted_by") or "").strip(),
                    (row.get("first_seen_date") or "").strip(),
                    (row.get("proposed_status") or "").strip() or "pending_review",
                    to_float((row.get("confidence") or "").strip()),
                    (row.get("topic_id") or "").strip(),
                    evidence_doc_id or None,
                    (row.get("evidence_url") or "").strip(),
                    (row.get("rationale") or "").strip(),
                    now_iso,
                    now_iso,
                ),
            )

        # Replace resolved alias map each run.
        conn.execute("DELETE FROM entity_aliases")
        existing_entity_ids = {row[0] for row in conn.execute("SELECT entity_id FROM entities")}
        for row in entity_alias_rows:
            canonical_name = (row.get("canonical_name") or "").strip()
            alias_name = (row.get("alias_name") or "").strip()
            if not canonical_name and not alias_name:
                continue
            alias_name = alias_name or canonical_name
            normalized_alias = (row.get("normalized_alias_name") or "").strip() or normalize_name(alias_name)
            entity_type = (row.get("entity_type") or "").strip() or classify_entity_type(canonical_name or alias_name)
            entity_id = (row.get("entity_id") or "").strip()
            if not entity_id:
                entity_id = hash_id("entity", f"{entity_type}|{normalize_name(canonical_name or alias_name)}")
            if entity_id not in existing_entity_ids:
                conn.execute(
                    """
                    INSERT INTO entities (
                      entity_id, entity_type, canonical_name, normalized_name,
                      notes, created_at_utc, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(entity_id) DO UPDATE SET
                      canonical_name = excluded.canonical_name,
                      normalized_name = excluded.normalized_name,
                      entity_type = excluded.entity_type,
                      updated_at_utc = excluded.updated_at_utc
                    """,
                    (
                        entity_id,
                        entity_type,
                        canonical_name or alias_name,
                        normalize_name(canonical_name or alias_name),
                        "Auto-created from entity alias ingest",
                        now_iso,
                        now_iso,
                    ),
                )
                existing_entity_ids.add(entity_id)

            raw_source_doc_id = (row.get("source_doc_id") or "").strip()
            source_doc_id = doc_id_redirects.get(raw_source_doc_id, raw_source_doc_id)
            source_url = (row.get("source_url") or "").strip()
            if source_doc_id and source_doc_id not in existing_docs:
                ensure_placeholder_document(conn, source_doc_id, source_url, run_id, now_iso)
                existing_docs.add(source_doc_id)

            alias_type = (row.get("alias_type") or "").strip() or "alternate"
            alias_id = hash_id("alias", f"{entity_id}|{normalized_alias}|{alias_type}")
            conn.execute(
                """
                INSERT INTO entity_aliases (
                  alias_id, entity_id, alias_name, normalized_alias_name, alias_type,
                  source_doc_id, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(alias_id) DO UPDATE SET
                  alias_name = excluded.alias_name,
                  normalized_alias_name = excluded.normalized_alias_name,
                  alias_type = excluded.alias_type,
                  source_doc_id = excluded.source_doc_id
                """,
                (
                    alias_id,
                    entity_id,
                    alias_name,
                    normalized_alias,
                    alias_type,
                    source_doc_id or None,
                    now_iso,
                ),
            )

        if current_doc_ids:
            placeholders = ",".join("?" for _ in current_doc_ids)
            conn.execute(f"DELETE FROM entity_mentions WHERE doc_id IN ({placeholders})", tuple(current_doc_ids))

        for row in entity_mention_rows:
            raw_doc_id = (row.get("doc_id") or "").strip()
            if not raw_doc_id:
                continue
            doc_id = doc_id_redirects.get(raw_doc_id, raw_doc_id)
            source_url = (row.get("source_url") or "").strip()
            if doc_id not in existing_docs:
                ensure_placeholder_document(conn, doc_id, source_url, run_id, now_iso)
                existing_docs.add(doc_id)

            mention_text = (row.get("mention_text") or "").strip()
            if not mention_text:
                continue
            entity_id = (row.get("entity_id") or "").strip()
            if not entity_id:
                entity_id = hash_id("entity", f"unknown|{normalize_name(mention_text)}")
            if entity_id not in existing_entity_ids:
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
                        (row.get("entity_type") or "").strip() or "unknown",
                        (row.get("canonical_name") or "").strip() or mention_text,
                        normalize_name((row.get("canonical_name") or "").strip() or mention_text),
                        "Auto-created from entity mentions ingest",
                        now_iso,
                        now_iso,
                    ),
                )
                existing_entity_ids.add(entity_id)

            context_snippet = (row.get("context_snippet") or "").strip()
            source_span_id = (row.get("source_span_id") or "").strip()
            if not source_span_id and context_snippet:
                source_span_id = hash_id(
                    "span",
                    "|".join(
                        [
                            doc_id,
                            (row.get("context_type") or "").strip(),
                            sha1_text(context_snippet),
                            source_url,
                        ]
                    ),
                )
            if source_span_id:
                snippet_hash = sha1_text(context_snippet) if context_snippet else ""
                conn.execute(
                    """
                    INSERT INTO evidence_spans (
                      span_id, doc_id, locator_type, locator_value, snippet_text,
                      snippet_hash, source_url, created_at_utc, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(span_id) DO UPDATE SET
                      snippet_text = excluded.snippet_text,
                      snippet_hash = excluded.snippet_hash,
                      source_url = excluded.source_url,
                      updated_at_utc = excluded.updated_at_utc
                    """,
                    (
                        source_span_id,
                        doc_id,
                        "unknown",
                        (row.get("context_type") or "").strip() or "unknown",
                        context_snippet,
                        snippet_hash,
                        source_url,
                        now_iso,
                        now_iso,
                    ),
                )

            mention_id = (row.get("mention_id") or "").strip()
            if not mention_id:
                mention_id = hash_id(
                    "mention",
                    "|".join(
                        [
                            doc_id,
                            entity_id,
                            normalize_name(mention_text),
                            (row.get("context_type") or "").strip(),
                            source_span_id,
                        ]
                    ),
                )

            mention_count_raw = (row.get("mention_count") or "").strip()
            try:
                mention_count = int(mention_count_raw) if mention_count_raw else 1
            except Exception:
                mention_count = 1

            confidence = to_float((row.get("confidence") or "").strip())
            if confidence is None:
                confidence = 0.5

            conn.execute(
                """
                INSERT INTO entity_mentions (
                  mention_id, doc_id, entity_id, mention_text, normalized_mention_text,
                  context_type, context_snippet, mention_count, confidence, source_span_id,
                  source_url, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mention_id) DO UPDATE SET
                  context_snippet = excluded.context_snippet,
                  mention_count = excluded.mention_count,
                  confidence = excluded.confidence,
                  source_span_id = excluded.source_span_id,
                  source_url = excluded.source_url,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    mention_id,
                    doc_id,
                    entity_id,
                    mention_text,
                    (row.get("normalized_mention_text") or "").strip() or normalize_name(mention_text),
                    (row.get("context_type") or "").strip() or "general_reference",
                    context_snippet,
                    mention_count,
                    confidence,
                    source_span_id or None,
                    source_url,
                    now_iso,
                    now_iso,
                ),
            )

        existing_claim_ids = {row[0] for row in conn.execute("SELECT claim_id FROM claims")}
        for row in claim_flag_rows:
            claim_id = (row.get("claim_id") or "").strip()
            rule_id = (row.get("rule_id") or "").strip()
            if not claim_id or not rule_id:
                continue
            if claim_id not in existing_claim_ids:
                continue
            related_doc_raw = (row.get("related_doc_id") or "").strip()
            related_doc_id = doc_id_redirects.get(related_doc_raw, related_doc_raw)
            related_source_url = (row.get("related_source_url") or "").strip()
            if related_doc_id and related_doc_id not in existing_docs:
                ensure_placeholder_document(conn, related_doc_id, related_source_url, run_id, now_iso)
                existing_docs.add(related_doc_id)

            flag_id = (row.get("flag_id") or "").strip()
            if not flag_id:
                flag_id = hash_id("flag", f"{claim_id}|{rule_id}|{related_doc_id}|{(row.get('message') or '').strip()}")

            conn.execute(
                """
                INSERT INTO claim_quality_flags (
                  flag_id, claim_id, rule_id, severity, flag_status, message,
                  related_doc_id, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(flag_id) DO UPDATE SET
                  severity = excluded.severity,
                  flag_status = excluded.flag_status,
                  message = excluded.message,
                  related_doc_id = excluded.related_doc_id,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    flag_id,
                    claim_id,
                    rule_id,
                    (row.get("severity") or "").strip() or "warn",
                    (row.get("flag_status") or "").strip() or "open",
                    (row.get("message") or "").strip() or rule_id,
                    related_doc_id or None,
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
    print(f"- Claim candidates loaded: {len(candidates)}")
    print(f"- Entity aliases loaded: {len(entity_alias_rows)}")
    print(f"- Entity mentions loaded: {len(entity_mention_rows)}")
    print(f"- Claim quality flags loaded: {len(claim_flag_rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
