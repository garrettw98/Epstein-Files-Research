#!/usr/bin/env python3
"""Build a redaction taxonomy report from current claim registry language."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
from collections import Counter

TAXONOMY = {
    "victim_privacy": "Redactions intended to protect survivor/victim identity.",
    "ongoing_investigation": "Redactions tied to active investigative sensitivity.",
    "national_security": "Redactions justified as intelligence/national-security sensitive.",
    "context_gap": "Name/context mismatch where list-level references lack per-name context.",
    "unknown": "Redaction rationale unclear or not yet classified in available records.",
}


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def classify(text: str) -> str:
    lowered = (text or "").lower()
    if any(token in lowered for token in ("victim", "survivor", "minor")):
        return "victim_privacy"
    if any(token in lowered for token in ("ongoing investigation", "pending investigation")):
        return "ongoing_investigation"
    if any(token in lowered for token in ("national security", "intelligence")):
        return "national_security"
    if any(token in lowered for token in ("redaction", "context", "listed", "names", "summary")):
        return "context_gap"
    return "unknown"


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Generate redaction taxonomy report.")
    parser.add_argument(
        "--claims-tsv",
        default=str(root / "derived" / "claims" / "claim_registry_latest.tsv"),
        help="Claim registry TSV path.",
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
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: list[list[str]] = []
    counts = Counter()
    for row in claims:
        claim_id = (row.get("claim_id") or "").strip()
        if not claim_id:
            continue
        claim_text = (row.get("claim_text") or "").strip()
        category = classify(claim_text)
        counts[category] += 1
        rows.append(
            [
                run_utc,
                claim_id,
                category,
                TAXONOMY.get(category, ""),
                (row.get("status") or "").strip(),
                (row.get("name_context_class") or "unknown").strip(),
                claim_text,
            ]
        )

    latest_tsv = out_dir / "redaction_taxonomy_latest.tsv"
    stamp_tsv = out_dir / f"redaction_taxonomy_{run_utc}.tsv"
    with latest_tsv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "claim_id",
                "redaction_category",
                "category_definition",
                "claim_status",
                "name_context_class",
                "claim_text",
            ]
        )
        writer.writerows(rows)
    stamp_tsv.write_text(latest_tsv.read_text(encoding="utf-8"), encoding="utf-8")

    latest_md = out_dir / "redaction_taxonomy_summary_latest.md"
    stamp_md = out_dir / f"redaction_taxonomy_summary_{run_utc}.md"
    with latest_md.open("w", encoding="utf-8") as handle:
        handle.write("# Redaction Taxonomy Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Claims classified: {len(rows)}\n\n")
        handle.write("## Taxonomy Counts\n")
        for key in ["victim_privacy", "ongoing_investigation", "national_security", "context_gap", "unknown"]:
            handle.write(f"- {key}: {counts.get(key, 0)}\n")

        handle.write("\n## Taxonomy Definitions\n")
        for key, value in TAXONOMY.items():
            handle.write(f"- {key}: {value}\n")

    stamp_md.write_text(latest_md.read_text(encoding="utf-8"), encoding="utf-8")

    print("Redaction taxonomy report complete.")
    print(f"- {latest_tsv}")
    print(f"- {latest_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
