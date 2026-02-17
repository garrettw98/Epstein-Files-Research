#!/usr/bin/env python3
"""Generate a coverage-gap dashboard for dataset completeness and source health."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import pathlib
import re
import urllib.parse
from collections import Counter


EXPECTED_PRIMARY_SOURCES = {
    "courtlistener",
    "courtlistener_recap",
    "house_judiciary",
    "govtrack",
    "govinfo",
    "govinfo_wssearch",
    "justice_opa",
}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_stamp() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def parse_compact_utc(value: str) -> dt.datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            parsed = dt.datetime.strptime(raw, fmt)
            return parsed.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    return None


def url_host(url: str) -> str:
    host = urllib.parse.urlsplit(url or "").netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def extract_expected_dataset_max(readme_text: str) -> int:
    nums = [int(match) for match in re.findall(r"Data Set\s+(\d+)", readme_text, flags=re.IGNORECASE)]
    return max(nums) if nums else 0


def extract_ingested_dataset_numbers(rows: list[dict[str, str]]) -> set[int]:
    found: set[int] = set()
    for row in rows:
        candidate = " ".join(
            [
                (row.get("url") or ""),
                (row.get("path") or ""),
                (row.get("snapshot_file") or ""),
            ]
        )
        for match in re.findall(r"(?:dataset|data[-_ ]set|set[-_ ]?)(\d+)", candidate, flags=re.IGNORECASE):
            try:
                found.add(int(match))
            except ValueError:
                pass
    return found


def read_manifest_run_utc(path: pathlib.Path) -> str:
    if not path.exists():
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if isinstance(payload, dict):
        return str(payload.get("run_utc") or payload.get("run_id") or "")
    return ""


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Generate coverage gap dashboard.")
    parser.add_argument(
        "--root",
        default=str(root),
        help="Repository root.",
    )
    parser.add_argument(
        "--stale-hours",
        type=int,
        default=36,
        help="Threshold (hours) for stale source warnings.",
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
    now = now_utc()

    root = pathlib.Path(args.root).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    readme_text = (root / "README.md").read_text(encoding="utf-8") if (root / "README.md").exists() else ""
    doj_index_rows = read_tsv(root / "derived" / "doj_epstein_library" / "epstein_library_index_latest.tsv")
    dataset_count_rows = read_tsv(root / "derived" / "doj_epstein_library" / "dataset_file_counts_latest.tsv")
    primary_docs_rows = read_tsv(root / "derived" / "primary_docs" / "primary_documents_latest.tsv")
    media_status_rows = read_tsv(root / "derived" / "media_coverage" / "outlet_endpoint_status_latest.tsv")
    claim_quality_rows = read_tsv(root / "derived" / "claims" / "claim_quality_flags_latest.tsv")

    expected_dataset_max = extract_expected_dataset_max(readme_text)
    ingested_dataset_numbers = extract_ingested_dataset_numbers(doj_index_rows)
    if expected_dataset_max > 0:
        expected_set = set(range(1, expected_dataset_max + 1))
        missing_datasets = sorted(expected_set - ingested_dataset_numbers)
    else:
        missing_datasets = []

    datasets_with_files: list[int] = []
    datasets_zero_files: list[int] = []
    dataset_total_files = 0
    for row in dataset_count_rows:
        try:
            dataset_number = int((row.get("dataset_number") or "").strip())
        except Exception:
            continue
        try:
            file_count = int((row.get("file_count") or "").strip() or 0)
        except Exception:
            file_count = 0
        dataset_total_files += file_count
        if file_count > 0:
            datasets_with_files.append(dataset_number)
        else:
            datasets_zero_files.append(dataset_number)

    broken_doj_links = []
    for row in doj_index_rows:
        status_code = (row.get("status_code") or "").strip()
        if not status_code or status_code in {"200", "301", "302"}:
            continue
        host = url_host((row.get("url") or "").strip())
        if host.endswith("justice.gov"):
            broken_doj_links.append(row)
    media_endpoint_failures = [
        row for row in media_status_rows if (row.get("http_status") or "") and (row.get("http_status") not in {"200", "301", "302"})
    ]

    observed_primary_sources = {to for to in ((row.get("source_system") or "").strip() for row in primary_docs_rows) if to}
    missing_primary_sources = sorted(EXPECTED_PRIMARY_SOURCES - observed_primary_sources)

    stale_inputs: list[tuple[str, str]] = []
    primary_manifest_stamp = read_manifest_run_utc(root / "raw" / "primary_docs" / "run_manifest_latest.json")
    universe_manifest_stamp = read_manifest_run_utc(root / "raw" / "epstein_universe" / "run_manifest_latest.json")
    doj_stamp = ""
    if doj_index_rows:
        doj_stamp = (doj_index_rows[0].get("ingested_at_utc") or "").strip()

    for label, stamp in (
        ("raw/primary_docs/run_manifest_latest.json", primary_manifest_stamp),
        ("raw/epstein_universe/run_manifest_latest.json", universe_manifest_stamp),
        ("derived/doj_epstein_library/epstein_library_index_latest.tsv", doj_stamp),
    ):
        parsed = parse_compact_utc(stamp)
        if not parsed:
            stale_inputs.append((label, "missing timestamp"))
            continue
        age_hours = (now - parsed).total_seconds() / 3600.0
        if age_hours > float(args.stale_hours):
            stale_inputs.append((label, f"{age_hours:.1f}h old"))

    by_severity = Counter((row.get("severity") or "").strip().lower() for row in claim_quality_rows)

    metrics: list[list[str]] = [
        ["expected_dataset_max", str(expected_dataset_max), "ok" if expected_dataset_max else "warn", "Max dataset number parsed from README live status."],
        [
            "ingested_dataset_count",
            str(len(ingested_dataset_numbers)),
            "ok" if ingested_dataset_numbers else "warn",
            f"Detected dataset numbers: {', '.join(str(x) for x in sorted(ingested_dataset_numbers)) or 'none'}",
        ],
        [
            "missing_dataset_count",
            str(len(missing_datasets)),
            "ok" if not missing_datasets else "warn",
            f"Missing datasets: {', '.join(str(x) for x in missing_datasets) if missing_datasets else 'none'}",
        ],
        [
            "dataset_sets_with_files",
            str(len(set(datasets_with_files))),
            "ok" if set(datasets_with_files) else "warn",
            f"Sets with >=1 file: {', '.join(str(x) for x in sorted(set(datasets_with_files))) if datasets_with_files else 'none'}",
        ],
        [
            "dataset_zero_file_sets",
            str(len(set(datasets_zero_files))),
            "ok" if not datasets_zero_files else "warn",
            f"Sets with 0 files: {', '.join(str(x) for x in sorted(set(datasets_zero_files))) if datasets_zero_files else 'none'}",
        ],
        [
            "dataset_total_files_indexed",
            str(dataset_total_files),
            "ok" if dataset_total_files > 0 else "warn",
            "Total files discovered across all data set listing pages.",
        ],
        [
            "doj_library_broken_links",
            str(len(broken_doj_links)),
            "ok" if not broken_doj_links else "warn",
            "Non-200/301/302 status codes in DOJ library index.",
        ],
        [
            "media_endpoint_failures",
            str(len(media_endpoint_failures)),
            "ok" if not media_endpoint_failures else "warn",
            "Outlet endpoints currently non-200/301/302.",
        ],
        [
            "primary_docs_total",
            str(len(primary_docs_rows)),
            "ok" if primary_docs_rows else "warn",
            "Current primary documents in latest snapshot.",
        ],
        [
            "missing_primary_source_systems",
            str(len(missing_primary_sources)),
            "ok" if not missing_primary_sources else "warn",
            f"Missing: {', '.join(missing_primary_sources) if missing_primary_sources else 'none'}",
        ],
        [
            "stale_inputs",
            str(len(stale_inputs)),
            "ok" if not stale_inputs else "warn",
            "; ".join(f"{label} ({detail})" for label, detail in stale_inputs) if stale_inputs else "none",
        ],
        [
            "claim_quality_high_flags",
            str(by_severity.get("high", 0)),
            "ok" if by_severity.get("high", 0) == 0 else "warn",
            "Open high-severity inference quality flags.",
        ],
    ]

    metrics_latest = out_dir / "coverage_gap_metrics_latest.tsv"
    metrics_stamp = out_dir / f"coverage_gap_metrics_{run_utc}.tsv"
    with metrics_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["metric", "value", "status", "detail"])
        writer.writerows(metrics)
    metrics_stamp.write_text(metrics_latest.read_text(encoding="utf-8"), encoding="utf-8")

    dashboard_latest = out_dir / "coverage_gap_dashboard_latest.md"
    dashboard_stamp = out_dir / f"coverage_gap_dashboard_{run_utc}.md"
    with dashboard_latest.open("w", encoding="utf-8") as handle:
        handle.write("# Coverage Gap Dashboard\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Expected dataset max: {expected_dataset_max or 'unknown'}\n")
        handle.write(f"- Ingested dataset numbers: {', '.join(str(x) for x in sorted(ingested_dataset_numbers)) or 'none detected'}\n")
        handle.write(f"- Missing datasets: {', '.join(str(x) for x in missing_datasets) if missing_datasets else 'none'}\n")
        handle.write(
            f"- Data sets with files: {', '.join(str(x) for x in sorted(set(datasets_with_files))) if datasets_with_files else 'none'}\n"
        )
        handle.write(
            f"- Data sets with zero files: {', '.join(str(x) for x in sorted(set(datasets_zero_files))) if datasets_zero_files else 'none'}\n"
        )
        handle.write(f"- Total data-set files indexed: {dataset_total_files}\n")
        handle.write(f"- Broken DOJ links: {len(broken_doj_links)}\n")
        handle.write(f"- Media endpoint failures: {len(media_endpoint_failures)}\n")
        handle.write(f"- Missing primary source systems: {', '.join(missing_primary_sources) if missing_primary_sources else 'none'}\n")
        handle.write(f"- Stale inputs: {len(stale_inputs)}\n")
        handle.write(f"- High claim-quality flags: {by_severity.get('high', 0)}\n\n")

        if datasets_zero_files:
            handle.write("## Data Set File Count Gaps\n")
            for value in sorted(set(datasets_zero_files)):
                handle.write(f"- Data Set {value}: zero files discovered from listing crawl\n")
            handle.write("\n")

        if broken_doj_links:
            handle.write("## Broken DOJ Links\n")
            for row in broken_doj_links[:25]:
                handle.write(f"- {row.get('status_code', '')}: {row.get('url', '')}\n")
            handle.write("\n")

        if media_endpoint_failures:
            handle.write("## Media Endpoint Failures\n")
            for row in media_endpoint_failures[:25]:
                handle.write(f"- {row.get('outlet', '')}: {row.get('http_status', '')} ({row.get('endpoint', '')})\n")
            handle.write("\n")

        if stale_inputs:
            handle.write("## Stale Inputs\n")
            for label, detail in stale_inputs:
                handle.write(f"- {label}: {detail}\n")

    dashboard_stamp.write_text(dashboard_latest.read_text(encoding="utf-8"), encoding="utf-8")

    print("Coverage gap dashboard generated.")
    print(f"- {dashboard_latest}")
    print(f"- {metrics_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
