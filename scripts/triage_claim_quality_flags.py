#!/usr/bin/env python3
"""Auto-triage claim quality flags into a prioritized review queue."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import pathlib
from collections import Counter


RULE_GAP_MAP = {
    "no_primary_evidence": "Missing tier-1 primary evidence link.",
    "no_direct_context": "Missing direct span-level context (locator/hash).",
    "name_only_implication_risk": "Name-only mention may overstate implication.",
    "unsupported_criminal_inference": "Criminal implication unsupported by direct primary evidence.",
    "status_upgrade_candidate": "Potential status upgrade requires reviewer confirmation.",
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


def hash_id(prefix: str, seed: str) -> str:
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def priority_from_counts(high_count: int, warn_count: int) -> str:
    if high_count > 0:
        return "p1"
    if warn_count > 0:
        return "p2"
    return "p3"


def recommended_action_for_rules(rule_ids: set[str]) -> str:
    if {"unsupported_criminal_inference", "name_only_implication_risk"} & rule_ids:
        return (
            "Rewrite claim language to neutral mention-only framing and require direct primary"
            " evidence before promotion."
        )
    if "no_primary_evidence" in rule_ids:
        return "Link a DOJ/court/congressional primary source or keep claim in pending_review."
    if "no_direct_context" in rule_ids:
        return "Add direct locator and snippet-hash context in claim evidence links."
    if "status_upgrade_candidate" in rule_ids:
        return "Reviewer confirmation needed before status upgrade."
    return "Manual quality review required."


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Build claim-quality triage queue.")
    parser.add_argument(
        "--flags-tsv",
        default=str(root / "derived" / "claims" / "claim_quality_flags_latest.tsv"),
        help="Claim quality flags TSV path.",
    )
    parser.add_argument(
        "--claims-tsv",
        default=str(root / "derived" / "claims" / "claim_registry_latest.tsv"),
        help="Claim registry TSV path.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(root / "derived" / "claims"),
        help="Output directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_utc = utc_stamp()

    flags_tsv = pathlib.Path(args.flags_tsv).resolve()
    claims_tsv = pathlib.Path(args.claims_tsv).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    flags = read_tsv(flags_tsv)
    claims = read_tsv(claims_tsv)
    claim_status_map = {(row.get("claim_id") or "").strip(): (row.get("status") or "").strip() for row in claims}

    flags_by_claim: dict[str, list[dict[str, str]]] = {}
    for row in flags:
        claim_id = (row.get("claim_id") or "").strip()
        if not claim_id:
            continue
        flag_status = (row.get("flag_status") or "").strip().lower()
        if flag_status and flag_status not in {"open", "in_review"}:
            continue
        flags_by_claim.setdefault(claim_id, []).append(row)

    queue_rows: list[list[str]] = []
    for claim_id in sorted(flags_by_claim):
        claim_flags = flags_by_claim[claim_id]
        severity_counts = Counter((row.get("severity") or "").strip().lower() for row in claim_flags)
        rule_ids = sorted({(row.get("rule_id") or "").strip() for row in claim_flags if (row.get("rule_id") or "").strip()})
        rule_set = set(rule_ids)

        high_count = severity_counts.get("high", 0)
        warn_count = severity_counts.get("warn", 0)
        priority = priority_from_counts(high_count, warn_count)
        evidence_gap = "; ".join(RULE_GAP_MAP.get(rule_id, rule_id) for rule_id in rule_ids) or "No rule metadata."
        recommended_action = recommended_action_for_rules(rule_set)

        related_doc_ids = sorted({(row.get("related_doc_id") or "").strip() for row in claim_flags if (row.get("related_doc_id") or "").strip()})
        related_source_urls = sorted(
            {(row.get("related_source_url") or "").strip() for row in claim_flags if (row.get("related_source_url") or "").strip()}
        )

        queue_id = hash_id("review", f"{claim_id}|{'|'.join(rule_ids)}")
        queue_rows.append(
            [
                run_utc,
                queue_id,
                claim_id,
                priority,
                claim_status_map.get(claim_id, ""),
                "open",
                str(len(claim_flags)),
                str(high_count),
                str(warn_count),
                ", ".join(rule_ids),
                to_tsv(evidence_gap),
                to_tsv(recommended_action),
                ", ".join(related_doc_ids),
                ", ".join(related_source_urls),
            ]
        )

    priority_order = {"p1": 1, "p2": 2, "p3": 3}
    queue_rows.sort(key=lambda row: (priority_order.get(row[3], 9), row[2]))

    latest_tsv = out_dir / "claim_review_queue_latest.tsv"
    stamp_tsv = out_dir / f"claim_review_queue_{run_utc}.tsv"
    with latest_tsv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "queue_id",
                "claim_id",
                "priority",
                "claim_status",
                "triage_status",
                "flag_count",
                "high_flag_count",
                "warn_flag_count",
                "rule_ids",
                "evidence_gap",
                "recommended_action",
                "related_doc_ids",
                "related_source_urls",
            ]
        )
        writer.writerows(queue_rows)
    stamp_tsv.write_text(latest_tsv.read_text(encoding="utf-8"), encoding="utf-8")

    summary_latest = out_dir / "claim_review_queue_summary_latest.md"
    summary_stamp = out_dir / f"claim_review_queue_summary_{run_utc}.md"
    by_priority = Counter(row[3] for row in queue_rows)
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Claim Review Queue Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Queue rows: {len(queue_rows)}\n")
        handle.write(f"- p1: {by_priority.get('p1', 0)}\n")
        handle.write(f"- p2: {by_priority.get('p2', 0)}\n")
        handle.write(f"- p3: {by_priority.get('p3', 0)}\n\n")
        handle.write("## Priority Queue (Top 20)\n")
        if not queue_rows:
            handle.write("- No open claim-quality flags to triage.\n")
        else:
            for row in queue_rows[:20]:
                handle.write(f"- {row[3]} | {row[2]} | {row[9]}\n")

    summary_stamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")

    print("Claim quality triage complete.")
    print(f"- {latest_tsv}")
    print(f"- {summary_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
