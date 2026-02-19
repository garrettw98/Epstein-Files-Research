#!/usr/bin/env python3
"""Generate a register of claims that lack tier-1 primary evidence links."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
from collections import Counter

PRIMARY_EVIDENCE_TYPES = {"primary", "transcript", "filing", "release"}


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
    parser = argparse.ArgumentParser(description="Build a primary-evidence gap register.")
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
        default=str(root / "derived" / "reports"),
        help="Output directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_utc = utc_stamp()

    claims = read_tsv(pathlib.Path(args.claims_tsv).resolve())
    links = read_tsv(pathlib.Path(args.links_tsv).resolve())
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    links_by_claim: dict[str, list[dict[str, str]]] = {}
    for row in links:
        claim_id = (row.get("claim_id") or "").strip()
        if claim_id:
            links_by_claim.setdefault(claim_id, []).append(row)

    rows: list[list[str]] = []
    for claim in claims:
        claim_id = (claim.get("claim_id") or "").strip()
        if not claim_id:
            continue
        claim_links = links_by_claim.get(claim_id, [])
        has_primary = any((row.get("evidence_type") or "").strip().lower() in PRIMARY_EVIDENCE_TYPES for row in claim_links)
        if has_primary:
            continue

        related_urls = sorted({(r.get("evidence_url") or "").strip() for r in claim_links if (r.get("evidence_url") or "").strip()})
        rows.append(
            [
                run_utc,
                claim_id,
                to_tsv(claim.get("status", "")),
                to_tsv(claim.get("claim_type", "")),
                to_tsv(claim.get("name_context_class", "unknown")),
                to_tsv(claim.get("asserted_by", "")),
                to_tsv(claim.get("first_seen_date", "")),
                to_tsv(claim.get("confidence", "")),
                to_tsv(claim.get("claim_text", "")),
                ", ".join(related_urls),
                "No tier-1 evidence_type link (primary/transcript/filing/release).",
            ]
        )

    rows.sort(key=lambda r: (r[2], r[1]))

    latest_tsv = out_dir / "primary_evidence_gap_register_latest.tsv"
    stamp_tsv = out_dir / f"primary_evidence_gap_register_{run_utc}.tsv"
    with latest_tsv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "claim_id",
                "claim_status",
                "claim_type",
                "name_context_class",
                "asserted_by",
                "first_seen_date",
                "confidence",
                "claim_text",
                "related_source_urls",
                "gap_reason",
            ]
        )
        writer.writerows(rows)
    stamp_tsv.write_text(latest_tsv.read_text(encoding="utf-8"), encoding="utf-8")

    by_status = Counter(r[2] for r in rows)
    by_context = Counter(r[4] for r in rows)

    latest_md = out_dir / "primary_evidence_gap_register_latest.md"
    stamp_md = out_dir / f"primary_evidence_gap_register_{run_utc}.md"
    with latest_md.open("w", encoding="utf-8") as handle:
        handle.write("# Primary Evidence Gap Register\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Claims lacking tier-1 links: {len(rows)}\n\n")

        handle.write("## By Claim Status\n")
        if not by_status:
            handle.write("- none\n")
        else:
            for key, value in sorted(by_status.items(), key=lambda item: (-item[1], item[0])):
                handle.write(f"- {key}: {value}\n")

        handle.write("\n## By Name Context Class\n")
        if not by_context:
            handle.write("- none\n")
        else:
            for key, value in sorted(by_context.items(), key=lambda item: (-item[1], item[0])):
                handle.write(f"- {key}: {value}\n")

        handle.write("\n## Claims (Top 50)\n")
        if not rows:
            handle.write("- none\n")
        else:
            for row in rows[:50]:
                handle.write(f"- {row[1]} | {row[2]} | {row[4]}\n")

    stamp_md.write_text(latest_md.read_text(encoding="utf-8"), encoding="utf-8")

    print("Primary evidence gap register complete.")
    print(f"- {latest_tsv}")
    print(f"- {latest_md}")
    print(f"- Rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
