#!/usr/bin/env python3
"""Flag claim quality risks (name-only inference, weak context, no primary evidence)."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import pathlib
from collections import Counter

PRIMARY_EVIDENCE_TYPES = {"primary", "transcript", "filing", "release"}


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def to_tsv(value: str) -> str:
    return (value or "").replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def hash_id(prefix: str, seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Assess claim context quality.")
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
        "--out-dir",
        default=str(root / "derived" / "claims"),
        help="Output directory.",
    )
    return parser.parse_args()


def has_name_only_pattern(text: str) -> bool:
    lowered = (text or "").lower()
    return any(token in lowered for token in ("listed", "mentions", "mentioned", "name", "names"))


def has_criminal_implication_pattern(text: str) -> bool:
    lowered = (text or "").lower()
    return any(
        token in lowered
        for token in (
            "incriminated",
            "criminal",
            "co-conspirator",
            "trafficking ring",
            "implicate",
            "implicated",
            "abused",
        )
    )


def main() -> int:
    args = parse_args()
    run_utc = utc_stamp()

    claims_tsv = pathlib.Path(args.claims_tsv).resolve()
    links_tsv = pathlib.Path(args.links_tsv).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    claims = read_tsv(claims_tsv)
    links = read_tsv(links_tsv)

    links_by_claim: dict[str, list[dict[str, str]]] = {}
    for row in links:
        claim_id = (row.get("claim_id") or "").strip()
        if not claim_id:
            continue
        links_by_claim.setdefault(claim_id, []).append(row)

    flags: list[list[str]] = []

    def add_flag(
        claim_id: str,
        rule_id: str,
        severity: str,
        message: str,
        related_doc_id: str,
        related_source_url: str,
    ) -> None:
        flag_id = hash_id("flag", f"{claim_id}|{rule_id}|{message}|{related_doc_id}")
        flags.append(
            [
                run_utc,
                flag_id,
                claim_id,
                rule_id,
                severity,
                "open",
                to_tsv(message),
                related_doc_id,
                related_source_url,
            ]
        )

    for claim in claims:
        claim_id = (claim.get("claim_id") or "").strip()
        if not claim_id:
            continue
        claim_text = (claim.get("claim_text") or "").strip()
        claim_status = (claim.get("status") or "").strip().lower()
        promoted_statuses = {"verified_primary", "verified_secondary", "disputed", "retracted"}
        requires_strict_evidence = claim_status in promoted_statuses
        claim_links = links_by_claim.get(claim_id, [])

        has_primary = any((row.get("evidence_type") or "").strip().lower() in PRIMARY_EVIDENCE_TYPES for row in claim_links)
        has_direct = any((row.get("evidence_strength") or "").strip().lower() == "direct" for row in claim_links)
        has_locator = any((row.get("evidence_locator") or "").strip() for row in claim_links)
        has_snippet_hash = any((row.get("snippet_hash") or "").strip() for row in claim_links)

        first_doc_id = ""
        first_source_url = ""
        if claim_links:
            first_doc_id = (claim_links[0].get("doc_id") or "").strip()
            first_source_url = (claim_links[0].get("evidence_url") or "").strip()

        if requires_strict_evidence and not has_primary:
            add_flag(
                claim_id=claim_id,
                rule_id="no_primary_evidence",
                severity="warn",
                message="Claim has no primary/transcript/filing/release evidence link.",
                related_doc_id=first_doc_id,
                related_source_url=first_source_url,
            )

        if requires_strict_evidence and not has_direct and not has_locator and not has_snippet_hash:
            add_flag(
                claim_id=claim_id,
                rule_id="no_direct_context",
                severity="warn",
                message="Claim lacks direct evidence strength and span-level locator/hash context.",
                related_doc_id=first_doc_id,
                related_source_url=first_source_url,
            )

        if requires_strict_evidence and has_name_only_pattern(claim_text) and not (has_primary and has_direct):
            add_flag(
                claim_id=claim_id,
                rule_id="name_only_implication_risk",
                severity="high",
                message="Name-list style claim without direct contextual evidence may overstate implication.",
                related_doc_id=first_doc_id,
                related_source_url=first_source_url,
            )

        if requires_strict_evidence and has_criminal_implication_pattern(claim_text) and not (has_primary and has_direct):
            add_flag(
                claim_id=claim_id,
                rule_id="unsupported_criminal_inference",
                severity="high",
                message="Claim implies criminal conduct but lacks direct primary evidence context.",
                related_doc_id=first_doc_id,
                related_source_url=first_source_url,
            )

        if claim_status == "verified_secondary" and has_primary:
            add_flag(
                claim_id=claim_id,
                rule_id="status_upgrade_candidate",
                severity="info",
                message="Claim has primary evidence and may qualify for verified_primary after review.",
                related_doc_id=first_doc_id,
                related_source_url=first_source_url,
            )

    latest_tsv = out_dir / "claim_quality_flags_latest.tsv"
    stamp_tsv = out_dir / f"claim_quality_flags_{run_utc}.tsv"
    with latest_tsv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "flag_id",
                "claim_id",
                "rule_id",
                "severity",
                "flag_status",
                "message",
                "related_doc_id",
                "related_source_url",
            ]
        )
        writer.writerows(flags)
    stamp_tsv.write_text(latest_tsv.read_text(encoding="utf-8"), encoding="utf-8")

    summary_latest = out_dir / "claim_quality_flags_summary_latest.md"
    summary_stamp = out_dir / f"claim_quality_flags_summary_{run_utc}.md"
    by_rule = Counter(row[3] for row in flags)
    by_severity = Counter(row[4] for row in flags)

    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Claim Quality Flag Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Claims inspected: {len(claims)}\n")
        handle.write(f"- Flags emitted: {len(flags)}\n\n")

        handle.write("## By Severity\n")
        if not by_severity:
            handle.write("- none\n")
        else:
            for key, value in sorted(by_severity.items(), key=lambda item: (-item[1], item[0])):
                handle.write(f"- {key}: {value}\n")

        handle.write("\n## By Rule\n")
        if not by_rule:
            handle.write("- none\n")
        else:
            for key, value in sorted(by_rule.items(), key=lambda item: (-item[1], item[0])):
                handle.write(f"- {key}: {value}\n")

    summary_stamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")

    print("Claim quality assessment complete.")
    print(f"- {latest_tsv}")
    print(f"- {summary_latest}")
    print(f"- Flags: {len(flags)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
