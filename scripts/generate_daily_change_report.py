#!/usr/bin/env python3
"""Generate daily diffs for primary docs and claim status changes."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
import re
import shutil
import sys
from collections import Counter


def now_utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def sorted_stamp_files(directory: pathlib.Path, prefix: str) -> list[pathlib.Path]:
    stamp_re = re.compile(rf"^{re.escape(prefix)}_\d{{8}}T\d{{6}}Z\.tsv$")
    return sorted(p for p in directory.glob(f"{prefix}_*.tsv") if p.is_file() and stamp_re.match(p.name))


def pick_latest_two(directory: pathlib.Path, prefix: str) -> tuple[pathlib.Path | None, pathlib.Path | None]:
    files = sorted_stamp_files(directory, prefix)
    if len(files) < 2:
        return None, files[-1] if files else None
    return files[-2], files[-1]


def write_tsv(path: pathlib.Path, headers: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Generate primary-doc and claim-status change reports.")
    parser.add_argument(
        "--primary-dir",
        default=str(root / "derived" / "primary_docs"),
        help="Directory containing primary_documents_<timestamp>.tsv files.",
    )
    parser.add_argument(
        "--claims-dir",
        default=str(root / "derived" / "claims"),
        help="Directory containing claim_registry_latest.tsv.",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(root / "derived" / "reports"),
        help="Directory for report outputs.",
    )
    return parser.parse_args()


def snapshot_claim_registry(claims_dir: pathlib.Path, run_utc: str) -> pathlib.Path:
    latest = claims_dir / "claim_registry_latest.tsv"
    if not latest.exists():
        return latest
    history_dir = claims_dir / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    snapshot = history_dir / f"claim_registry_{run_utc}.tsv"
    shutil.copyfile(latest, snapshot)
    return snapshot


def main() -> int:
    args = parse_args()
    run_utc = now_utc_stamp()

    primary_dir = pathlib.Path(args.primary_dir).resolve()
    claims_dir = pathlib.Path(args.claims_dir).resolve()
    reports_dir = pathlib.Path(args.reports_dir).resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Snapshot current claim registry so changes can be tracked over time.
    snapshot_claim_registry(claims_dir, run_utc)

    # Compare primary documents between latest two runs.
    prev_primary, latest_primary = pick_latest_two(primary_dir, "primary_documents")
    primary_diff_rows: list[list[str]] = []
    if prev_primary and latest_primary:
        prev_docs = {row.get("doc_id", ""): row for row in read_tsv(prev_primary)}
        latest_docs = {row.get("doc_id", ""): row for row in read_tsv(latest_primary)}

        for doc_id, row in sorted(latest_docs.items()):
            if doc_id not in prev_docs:
                primary_diff_rows.append(
                    [
                        "added",
                        doc_id,
                        row.get("source_system", ""),
                        row.get("document_type", ""),
                        row.get("doc_date", ""),
                        row.get("status", ""),
                        row.get("url", ""),
                        row.get("title", ""),
                    ]
                )

        for doc_id, row in sorted(prev_docs.items()):
            if doc_id not in latest_docs:
                primary_diff_rows.append(
                    [
                        "removed",
                        doc_id,
                        row.get("source_system", ""),
                        row.get("document_type", ""),
                        row.get("doc_date", ""),
                        row.get("status", ""),
                        row.get("url", ""),
                        row.get("title", ""),
                    ]
                )

        tracked_fields = ["title", "doc_date", "status", "source_system", "document_type", "url"]
        for doc_id, latest_row in sorted(latest_docs.items()):
            prev_row = prev_docs.get(doc_id)
            if not prev_row:
                continue
            field_changes = []
            for field in tracked_fields:
                old = (prev_row.get(field) or "").strip()
                new = (latest_row.get(field) or "").strip()
                if old != new:
                    field_changes.append(f"{field}: '{old}' -> '{new}'")
            if field_changes:
                primary_diff_rows.append(
                    [
                        "changed",
                        doc_id,
                        latest_row.get("source_system", ""),
                        latest_row.get("document_type", ""),
                        latest_row.get("doc_date", ""),
                        latest_row.get("status", ""),
                        latest_row.get("url", ""),
                        "; ".join(field_changes),
                    ]
                )

    primary_headers = [
        "change_type",
        "doc_id",
        "source_system",
        "document_type",
        "doc_date",
        "status",
        "url",
        "detail",
    ]
    primary_latest_out = reports_dir / "daily_primary_doc_diff_latest.tsv"
    primary_stamp_out = reports_dir / f"daily_primary_doc_diff_{run_utc}.tsv"
    write_tsv(primary_latest_out, primary_headers, primary_diff_rows)
    shutil.copyfile(primary_latest_out, primary_stamp_out)

    # Compare claims across snapshots.
    claim_history_dir = claims_dir / "history"
    prev_claims_file, latest_claims_file = pick_latest_two(claim_history_dir, "claim_registry")
    claim_diff_rows: list[list[str]] = []
    if prev_claims_file and latest_claims_file:
        prev_claims = {row.get("claim_id", ""): row for row in read_tsv(prev_claims_file)}
        latest_claims = {row.get("claim_id", ""): row for row in read_tsv(latest_claims_file)}

        claim_ids = sorted(set(prev_claims) | set(latest_claims))
        for claim_id in claim_ids:
            old = prev_claims.get(claim_id)
            new = latest_claims.get(claim_id)
            if old is None:
                claim_diff_rows.append(
                    [
                        "added",
                        claim_id,
                        "",
                        new.get("status", ""),
                        "",
                        new.get("confidence", ""),
                        new.get("claim_text", ""),
                    ]
                )
                continue
            if new is None:
                claim_diff_rows.append(
                    [
                        "removed",
                        claim_id,
                        old.get("status", ""),
                        "",
                        old.get("confidence", ""),
                        "",
                        old.get("claim_text", ""),
                    ]
                )
                continue

            old_status = (old.get("status") or "").strip()
            new_status = (new.get("status") or "").strip()
            old_conf = (old.get("confidence") or "").strip()
            new_conf = (new.get("confidence") or "").strip()
            old_text = (old.get("claim_text") or "").strip()
            new_text = (new.get("claim_text") or "").strip()
            if old_status != new_status or old_conf != new_conf or old_text != new_text:
                claim_diff_rows.append(
                    [
                        "changed",
                        claim_id,
                        old_status,
                        new_status,
                        old_conf,
                        new_conf,
                        new_text,
                    ]
                )

    claim_headers = [
        "change_type",
        "claim_id",
        "previous_status",
        "new_status",
        "previous_confidence",
        "new_confidence",
        "claim_text",
    ]
    claim_latest_out = reports_dir / "daily_claim_status_changes_latest.tsv"
    claim_stamp_out = reports_dir / f"daily_claim_status_changes_{run_utc}.tsv"
    write_tsv(claim_latest_out, claim_headers, claim_diff_rows)
    shutil.copyfile(claim_latest_out, claim_stamp_out)

    quality_flags = read_tsv(claims_dir / "claim_quality_flags_latest.tsv")
    quality_by_severity = Counter((row.get("severity") or "").strip().lower() for row in quality_flags if row.get("severity"))

    summary_latest = reports_dir / "daily_change_report_latest.md"
    summary_stamp = reports_dir / f"daily_change_report_{run_utc}.md"
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Daily Change Report\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Primary documents compared: {prev_primary.name if prev_primary else 'n/a'} -> {latest_primary.name if latest_primary else 'n/a'}\n")
        handle.write(
            f"- Claim snapshots compared: {prev_claims_file.name if prev_claims_file else 'n/a'} -> {latest_claims_file.name if latest_claims_file else 'n/a'}\n"
        )
        handle.write(f"- Primary doc changes: {len(primary_diff_rows)}\n")
        handle.write(f"- Claim changes: {len(claim_diff_rows)}\n\n")

        handle.write("## Primary Doc Change Samples\n")
        if not primary_diff_rows:
            handle.write("- No detected changes between latest two primary document snapshots.\n")
        else:
            for row in primary_diff_rows[:20]:
                handle.write(f"- {row[0]}: {row[1]} ({row[2]})\n")

        handle.write("\n## Claim Change Samples\n")
        if not claim_diff_rows:
            handle.write("- No detected claim registry changes between latest two snapshots.\n")
        else:
            for row in claim_diff_rows[:20]:
                handle.write(f"- {row[0]}: {row[1]} ({row[2]} -> {row[3]})\n")

        handle.write("\n## Claim Quality Flags (Latest)\n")
        if not quality_flags:
            handle.write("- No claim quality flag file found for this run.\n")
        else:
            total = len(quality_flags)
            handle.write(f"- Total flags: {total}\n")
            for severity in ("high", "warn", "info"):
                handle.write(f"- {severity}: {quality_by_severity.get(severity, 0)}\n")

    shutil.copyfile(summary_latest, summary_stamp)

    print("Daily change report generated.")
    print(f"- {summary_latest}")
    print(f"- {primary_latest_out}")
    print(f"- {claim_latest_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
