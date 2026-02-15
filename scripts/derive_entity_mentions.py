#!/usr/bin/env python3
"""Derive canonical entity aliases and context-typed entity mentions."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import difflib
import hashlib
import pathlib
import re
from collections import Counter
from urllib.parse import urlparse

STOP_PHRASES = {
    "department of justice",
    "house judiciary committee",
    "us congress",
    "public law",
    "federal law",
    "executive session",
    "executive calendar",
    "congressional record",
    "district court",
    "court of appeals",
    "press release",
}

NEWS_DOMAINS = {
    "apnews.com",
    "reuters.com",
    "yahoo.com",
    "cbsnews.com",
    "cnn.com",
    "foxnews.com",
    "bbc.com",
    "theguardian.com",
    "nypost.com",
}

DOC_CONTEXT_OVERRIDES = {
    "court_record": "legal_filing",
    "pacer_recap_document": "legal_filing",
    "congressional_hearing": "legal_filing",
    "congressional_hearing_transcript": "legal_filing",
    "congressional_record_transcript": "legal_filing",
    "congressional_bill": "legal_filing",
    "bill_action": "legal_filing",
    "public_law": "legal_filing",
    "agency_release": "legal_filing",
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


def normalize_name(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in value)
    return " ".join(cleaned.split())


def hash_id(prefix: str, seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def infer_entity_type(name: str) -> str:
    lower = name.lower()
    if any(token in lower for token in ("committee", "department", "agency", "office", "court", "senate", "house", "congress")):
        return "organization"
    words = [w for w in re.split(r"\s+", name.strip()) if w]
    if any(len(w) <= 4 and w.isupper() for w in words):
        return "organization"
    if len(words) >= 2 and all(w[0:1].isupper() for w in words if w[0:1].isalpha()):
        return "person"
    return "organization"


def profile_names(profiles_dir: pathlib.Path) -> list[str]:
    names: list[str] = []
    if not profiles_dir.exists():
        return names
    for path in sorted(profiles_dir.glob("*.md")):
        if path.name == "README.md":
            continue
        names.append(path.stem.replace("_", " "))
    return names


def extract_name_phrases(text: str) -> list[str]:
    if not text:
        return []
    matches = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b", text)
    unique: list[str] = []
    seen: set[str] = set()
    for raw in matches:
        name = to_tsv(raw)
        norm = normalize_name(name)
        if not norm or norm in STOP_PHRASES or norm in seen:
            continue
        seen.add(norm)
        unique.append(name)
    return unique


def classify_link_context(claim_type: str, evidence_type: str, text: str, url: str) -> str:
    lowered = (text or "").lower()
    claim_type = (claim_type or "").lower()
    evidence_type = (evidence_type or "").lower()
    domain = (urlparse(url).netloc or "").lower()
    if "from:" in lowered and "email" in lowered:
        return "email_sender"
    if "email" in lowered or "inbox" in lowered:
        return "email_body"
    if any(token in lowered for token in ("flight", "manifest", "plane", "lolita express")):
        return "flight_log"
    if claim_type == "allegation" or any(token in lowered for token in ("alleged", "accused", "allegation")):
        return "allegation"
    if domain.endswith(tuple(NEWS_DOMAINS)) or evidence_type == "secondary":
        return "news_clipping"
    if evidence_type in {"primary", "transcript", "filing", "release"}:
        return "legal_filing"
    return "general_reference"


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Derive canonical entity aliases and mentions.")
    parser.add_argument(
        "--docs-tsv",
        default=str(root / "derived" / "primary_docs" / "primary_documents_latest.tsv"),
        help="Primary docs TSV path.",
    )
    parser.add_argument(
        "--claims-tsv",
        default=str(root / "derived" / "claims" / "claim_registry_latest.tsv"),
        help="Claim registry TSV path.",
    )
    parser.add_argument(
        "--links-tsv",
        default=str(root / "derived" / "claims" / "claim_evidence_links_latest.tsv"),
        help="Claim evidence links TSV path.",
    )
    parser.add_argument(
        "--profiles-dir",
        default=str(root / "profiles"),
        help="Profiles directory used for canonical seed names.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(root / "derived" / "entities"),
        help="Output directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_utc = utc_stamp()

    docs_tsv = pathlib.Path(args.docs_tsv).resolve()
    claims_tsv = pathlib.Path(args.claims_tsv).resolve()
    links_tsv = pathlib.Path(args.links_tsv).resolve()
    profiles_dir = pathlib.Path(args.profiles_dir).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    docs = read_tsv(docs_tsv)
    claims = read_tsv(claims_tsv)
    links = read_tsv(links_tsv)

    claim_type_by_id = {(row.get("claim_id") or "").strip(): (row.get("claim_type") or "").strip() for row in claims}

    canonical_entities: dict[str, dict[str, str]] = {}
    alias_rows: list[list[str]] = []
    alias_seen: set[tuple[str, str, str]] = set()

    def resolve_entity(alias_name: str, alias_type: str, source: str) -> tuple[str, str, str, str]:
        alias_name_clean = to_tsv(alias_name)
        norm = normalize_name(alias_name_clean)
        if not norm:
            return "", "", "", ""

        if norm in canonical_entities:
            entity = canonical_entities[norm]
            resolution_method = "exact"
            similarity = "1.00"
        else:
            best_norm = ""
            best_score = 0.0
            for existing_norm in canonical_entities:
                score = difflib.SequenceMatcher(a=norm, b=existing_norm).ratio()
                if score > best_score:
                    best_score = score
                    best_norm = existing_norm
            if best_norm and best_score >= 0.93:
                entity = canonical_entities[best_norm]
                resolution_method = "fuzzy"
                similarity = f"{best_score:.2f}"
            else:
                canonical_name = alias_name_clean
                entity_type = infer_entity_type(canonical_name)
                entity_id = hash_id("entity", f"{entity_type}|{norm}")
                entity = {
                    "entity_id": entity_id,
                    "canonical_name": canonical_name,
                    "normalized_name": norm,
                    "entity_type": entity_type,
                }
                canonical_entities[norm] = entity
                resolution_method = "canonical_create"
                similarity = "1.00"

        key = (entity["entity_id"], norm, alias_type)
        if key not in alias_seen:
            alias_seen.add(key)
            alias_rows.append(
                [
                    run_utc,
                    entity["entity_id"],
                    entity["entity_type"],
                    entity["canonical_name"],
                    alias_name_clean,
                    norm,
                    alias_type,
                    resolution_method,
                    similarity,
                    source,
                    "",
                    "",
                ]
            )

        return entity["entity_id"], entity["canonical_name"], entity["entity_type"], norm

    for name in profile_names(profiles_dir):
        resolve_entity(name, "profile_slug", "profiles")

    for row in claims:
        asserted_by = (row.get("asserted_by") or "").strip()
        if asserted_by:
            resolve_entity(asserted_by, "asserted_by", "claim_registry")
        for phrase in extract_name_phrases((row.get("claim_text") or "").strip()):
            resolve_entity(phrase, "extracted_variant", "claim_registry")

    for row in links:
        for phrase in extract_name_phrases((row.get("quote_excerpt") or "").strip()):
            resolve_entity(phrase, "extracted_variant", "claim_evidence")

    # Ensure every canonical name has a canonical alias row.
    for norm, entity in list(canonical_entities.items()):
        key = (entity["entity_id"], norm, "canonical")
        if key in alias_seen:
            continue
        alias_seen.add(key)
        alias_rows.append(
            [
                run_utc,
                entity["entity_id"],
                entity["entity_type"],
                entity["canonical_name"],
                entity["canonical_name"],
                norm,
                "canonical",
                "exact",
                "1.00",
                "canonical_seed",
                "",
                "",
            ]
        )

    alias_rows_sorted = sorted(alias_rows, key=lambda row: (row[3].lower(), row[4].lower(), row[6]))

    # Mention extraction.
    alias_lookup: list[dict[str, str]] = []
    for row in alias_rows_sorted:
        alias_name = row[4]
        if len(alias_name.split()) < 2:
            continue
        alias_lookup.append(
            {
                "entity_id": row[1],
                "canonical_name": row[3],
                "entity_type": row[2],
                "alias_name": alias_name,
                "normalized_alias": row[5],
                "alias_type": row[6],
            }
        )

    mention_agg: dict[tuple[str, str, str, str, str], dict[str, object]] = {}

    def add_mention(
        doc_id: str,
        entity_id: str,
        canonical_name: str,
        entity_type: str,
        alias_name: str,
        context_type: str,
        snippet: str,
        source_url: str,
        confidence: float,
        mention_count: int,
    ) -> None:
        norm_alias = normalize_name(alias_name)
        key = (doc_id, entity_id, norm_alias, context_type, source_url)
        existing = mention_agg.get(key)
        if existing is None:
            mention_agg[key] = {
                "doc_id": doc_id,
                "entity_id": entity_id,
                "canonical_name": canonical_name,
                "entity_type": entity_type,
                "mention_text": alias_name,
                "normalized_mention_text": norm_alias,
                "context_type": context_type,
                "context_snippet": to_tsv(snippet)[:320],
                "mention_count": mention_count,
                "confidence": confidence,
                "source_url": source_url,
            }
            return
        existing["mention_count"] = int(existing["mention_count"]) + mention_count
        existing["confidence"] = max(float(existing["confidence"]), confidence)
        if not existing["context_snippet"] and snippet:
            existing["context_snippet"] = to_tsv(snippet)[:320]

    for row in docs:
        doc_id = (row.get("doc_id") or "").strip()
        if not doc_id:
            continue
        text = " ".join(
            [
                (row.get("title") or ""),
                (row.get("citation") or ""),
                (row.get("entity_tags") or ""),
                (row.get("extracted_from") or ""),
            ]
        )
        if not text.strip():
            continue
        lowered = text.lower()
        context_type = DOC_CONTEXT_OVERRIDES.get((row.get("document_type") or "").strip(), "general_reference")
        for alias in alias_lookup:
            alias_text = alias["alias_name"]
            pattern = rf"\b{re.escape(alias_text)}\b"
            count = len(re.findall(pattern, text, flags=re.IGNORECASE))
            if count <= 0:
                continue
            snippet = text[:320]
            confidence = 0.92 if alias["alias_type"] in {"canonical", "profile_slug"} else 0.82
            add_mention(
                doc_id=doc_id,
                entity_id=alias["entity_id"],
                canonical_name=alias["canonical_name"],
                entity_type=alias["entity_type"],
                alias_name=alias_text,
                context_type=context_type,
                snippet=snippet,
                source_url=(row.get("url") or "").strip(),
                confidence=confidence,
                mention_count=count,
            )

    for row in links:
        doc_id = (row.get("doc_id") or "").strip()
        if not doc_id:
            continue
        quote_excerpt = (row.get("quote_excerpt") or "").strip()
        if not quote_excerpt:
            continue
        claim_id = (row.get("claim_id") or "").strip()
        claim_type = claim_type_by_id.get(claim_id, "")
        context_type = classify_link_context(
            claim_type=claim_type,
            evidence_type=(row.get("evidence_type") or ""),
            text=quote_excerpt,
            url=(row.get("evidence_url") or ""),
        )
        for alias in alias_lookup:
            alias_text = alias["alias_name"]
            pattern = rf"\b{re.escape(alias_text)}\b"
            count = len(re.findall(pattern, quote_excerpt, flags=re.IGNORECASE))
            if count <= 0:
                continue
            confidence = 0.9 if context_type != "general_reference" else 0.8
            add_mention(
                doc_id=doc_id,
                entity_id=alias["entity_id"],
                canonical_name=alias["canonical_name"],
                entity_type=alias["entity_type"],
                alias_name=alias_text,
                context_type=context_type,
                snippet=quote_excerpt,
                source_url=(row.get("evidence_url") or "").strip(),
                confidence=confidence,
                mention_count=count,
            )

    mention_rows: list[list[str]] = []
    for payload in sorted(mention_agg.values(), key=lambda item: (str(item["doc_id"]), str(item["canonical_name"]), str(item["context_type"]))):
        mention_id = hash_id(
            "mention",
            "|".join(
                [
                    str(payload["doc_id"]),
                    str(payload["entity_id"]),
                    str(payload["normalized_mention_text"]),
                    str(payload["context_type"]),
                    str(payload["source_url"]),
                ]
            ),
        )
        mention_rows.append(
            [
                run_utc,
                mention_id,
                str(payload["doc_id"]),
                str(payload["entity_id"]),
                str(payload["entity_type"]),
                str(payload["canonical_name"]),
                str(payload["mention_text"]),
                str(payload["normalized_mention_text"]),
                str(payload["context_type"]),
                str(payload["context_snippet"]),
                str(payload["mention_count"]),
                f"{float(payload['confidence']):.2f}",
                "",
                str(payload["source_url"]),
            ]
        )

    aliases_latest = out_dir / "entity_aliases_resolved_latest.tsv"
    aliases_stamp = out_dir / f"entity_aliases_resolved_{run_utc}.tsv"
    with aliases_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "entity_id",
                "entity_type",
                "canonical_name",
                "alias_name",
                "normalized_alias_name",
                "alias_type",
                "resolution_method",
                "similarity",
                "source",
                "source_doc_id",
                "source_url",
            ]
        )
        writer.writerows(alias_rows_sorted)
    aliases_stamp.write_text(aliases_latest.read_text(encoding="utf-8"), encoding="utf-8")

    mentions_latest = out_dir / "entity_mentions_latest.tsv"
    mentions_stamp = out_dir / f"entity_mentions_{run_utc}.tsv"
    with mentions_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "mention_id",
                "doc_id",
                "entity_id",
                "entity_type",
                "canonical_name",
                "mention_text",
                "normalized_mention_text",
                "context_type",
                "context_snippet",
                "mention_count",
                "confidence",
                "source_span_id",
                "source_url",
            ]
        )
        writer.writerows(mention_rows)
    mentions_stamp.write_text(mentions_latest.read_text(encoding="utf-8"), encoding="utf-8")

    summary_latest = out_dir / "entity_mentions_summary_latest.md"
    summary_stamp = out_dir / f"entity_mentions_summary_{run_utc}.md"
    by_context = Counter(row[8] for row in mention_rows)
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Entity Mention Derivation Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Canonical entities: {len(canonical_entities)}\n")
        handle.write(f"- Aliases: {len(alias_rows_sorted)}\n")
        handle.write(f"- Mentions: {len(mention_rows)}\n\n")
        handle.write("## Mentions by Context Type\n")
        if not by_context:
            handle.write("- none\n")
        else:
            for key, value in sorted(by_context.items(), key=lambda item: (-item[1], item[0])):
                handle.write(f"- {key}: {value}\n")

    summary_stamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")

    print("Entity mention derivation complete.")
    print(f"- {aliases_latest}")
    print(f"- {mentions_latest}")
    print(f"- {summary_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
