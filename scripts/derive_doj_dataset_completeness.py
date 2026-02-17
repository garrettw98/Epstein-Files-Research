#!/usr/bin/env python3
"""Build per-data-set file count completeness outputs from DOJ disclosure pages."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import pathlib
import re
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict


USER_AGENT = "Mozilla/5.0 (EpsteinDatasetCompleteness/1.0)"
DATASET_PAGE_RE = re.compile(r"^/epstein/doj-disclosures/data-set-(\d+)-files/?$", re.IGNORECASE)
DATASET_FILE_RE = re.compile(r"/epstein/files/dataset\s*(\d+)/.+\.(pdf|zip|csv|json)$", re.IGNORECASE)
HREF_RE = re.compile(r"""href=['"]([^'"]+)['"]""", re.IGNORECASE)


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def to_tsv(value: str) -> str:
    return (value or "").replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()


def read_tsv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def extract_dataset_number_from_url(url: str) -> int | None:
    path = urllib.parse.unquote(urllib.parse.urlsplit(url or "").path)
    match = DATASET_PAGE_RE.search(path)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def extract_dataset_number_from_file_url(url: str) -> int | None:
    path = urllib.parse.unquote(urllib.parse.urlsplit(url or "").path)
    match = DATASET_FILE_RE.search(path)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def is_dataset_file_url(url: str) -> bool:
    return extract_dataset_number_from_file_url(url) is not None


def canonicalize_dataset_page_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url or "")
    page_only = [
        (key, value)
        for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() == "page"
    ]
    query = urllib.parse.urlencode(sorted(page_only))
    return urllib.parse.urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.rstrip("/"),
            query,
            "",
        )
    )


def fetch_text(url: str, timeout: int) -> tuple[int, str]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8", errors="replace")
            status = int(getattr(response, "status", 200) or 200)
            return status, payload
    except urllib.error.HTTPError as err:
        return int(err.code or 0), ""
    except Exception:
        return 0, ""


def extract_links(base_url: str, html_text: str) -> list[str]:
    links: list[str] = []
    for raw_link in HREF_RE.findall(html_text or ""):
        link = html.unescape((raw_link or "").strip())
        if not link:
            continue
        lower = link.lower()
        if lower.startswith(("javascript:", "mailto:", "#")):
            continue
        absolute = urllib.parse.urljoin(base_url, link)
        split = urllib.parse.urlsplit(absolute)
        if split.scheme not in {"http", "https"}:
            continue
        cleaned = urllib.parse.urlunsplit((split.scheme, split.netloc, split.path, split.query, ""))
        links.append(cleaned)
    return links


def parse_args() -> argparse.Namespace:
    root = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Derive DOJ dataset file-count completeness metrics.")
    parser.add_argument(
        "--index-tsv",
        default=str(root / "derived" / "doj_epstein_library" / "epstein_library_index_latest.tsv"),
        help="DOJ library index TSV path.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(root / "derived" / "doj_epstein_library"),
        help="Output directory.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--max-pages-per-dataset",
        type=int,
        default=40,
        help="Safety cap for paginated listing pages per dataset.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_utc = utc_stamp()

    index_tsv = pathlib.Path(args.index_tsv).resolve()
    out_dir = pathlib.Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    index_rows = read_tsv(index_tsv)
    dataset_pages: dict[int, str] = {}
    for row in index_rows:
        url = (row.get("url") or "").strip()
        dataset_num = extract_dataset_number_from_url(url)
        if dataset_num is None:
            continue
        dataset_pages[dataset_num] = url

    counts_rows: list[list[str]] = []
    file_index_rows: list[list[str]] = []

    for dataset_num in sorted(dataset_pages):
        root_url = dataset_pages[dataset_num]
        queue: list[str] = [root_url]
        visited: set[str] = set()
        file_to_page: dict[str, str] = {}
        errors: list[str] = []

        while queue and len(visited) < max(1, args.max_pages_per_dataset):
            page_url = queue.pop(0)
            canonical = canonicalize_dataset_page_url(page_url)
            if canonical in visited:
                continue
            visited.add(canonical)

            status, html_text = fetch_text(page_url, args.timeout)
            if status != 200 or not html_text:
                errors.append(f"{status}:{page_url}")
                continue

            for link in extract_links(page_url, html_text):
                page_dataset = extract_dataset_number_from_url(link)
                if page_dataset == dataset_num:
                    candidate = canonicalize_dataset_page_url(link)
                    if candidate not in visited and candidate not in queue:
                        queue.append(link)
                    continue

                if not is_dataset_file_url(link):
                    continue
                file_dataset = extract_dataset_number_from_file_url(link)
                if file_dataset not in {None, dataset_num}:
                    continue
                file_to_page.setdefault(link, page_url)

        unique_files = sorted(file_to_page)
        for file_url in unique_files:
            file_name = pathlib.PurePosixPath(urllib.parse.urlsplit(file_url).path).name
            file_index_rows.append(
                [
                    run_utc,
                    str(dataset_num),
                    root_url,
                    file_to_page[file_url],
                    file_url,
                    file_name,
                ]
            )

        status_label = "ok"
        if not unique_files:
            status_label = "alert"
        elif errors:
            status_label = "warn"

        counts_rows.append(
            [
                run_utc,
                str(dataset_num),
                root_url,
                str(len(visited)),
                str(len(unique_files)),
                str(len(errors)),
                "; ".join(errors[:5]),
                unique_files[0] if unique_files else "",
                unique_files[-1] if unique_files else "",
                status_label,
            ]
        )

    counts_latest = out_dir / "dataset_file_counts_latest.tsv"
    counts_stamp = out_dir / f"dataset_file_counts_{run_utc}.tsv"
    with counts_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "dataset_number",
                "dataset_url",
                "pages_fetched",
                "file_count",
                "http_error_count",
                "http_error_samples",
                "first_file_url",
                "last_file_url",
                "status",
            ]
        )
        writer.writerows(counts_rows)
    counts_stamp.write_text(counts_latest.read_text(encoding="utf-8"), encoding="utf-8")

    file_index_latest = out_dir / "dataset_file_index_latest.tsv"
    file_index_stamp = out_dir / f"dataset_file_index_{run_utc}.tsv"
    with file_index_latest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "run_utc",
                "dataset_number",
                "dataset_url",
                "listing_page_url",
                "file_url",
                "file_name",
            ]
        )
        writer.writerows(file_index_rows)
    file_index_stamp.write_text(file_index_latest.read_text(encoding="utf-8"), encoding="utf-8")

    totals_by_status = defaultdict(int)
    total_files = 0
    for row in counts_rows:
        totals_by_status[row[-1]] += 1
        try:
            total_files += int(row[4])
        except Exception:
            pass

    zero_file_sets = [row[1] for row in counts_rows if row[4] == "0"]

    summary_latest = out_dir / "dataset_file_counts_summary_latest.md"
    summary_stamp = out_dir / f"dataset_file_counts_summary_{run_utc}.md"
    with summary_latest.open("w", encoding="utf-8") as handle:
        handle.write("# DOJ Data Set File Completeness Summary\n\n")
        handle.write(f"- Run UTC: {run_utc}\n")
        handle.write(f"- Data sets discovered: {len(counts_rows)}\n")
        handle.write(f"- Total files indexed across sets: {total_files}\n")
        handle.write(f"- Data sets with zero files: {', '.join(zero_file_sets) if zero_file_sets else 'none'}\n")
        handle.write(
            "- Status counts: "
            + ", ".join(f"{label}={count}" for label, count in sorted(totals_by_status.items()))
            + "\n\n"
        )

        handle.write("| Data Set | Pages | Files | HTTP Errors | Status |\n")
        handle.write("| --- | --- | --- | --- | --- |\n")
        for row in counts_rows:
            handle.write(
                f"| {to_tsv(row[1])} | {to_tsv(row[3])} | {to_tsv(row[4])} | {to_tsv(row[5])} | {to_tsv(row[9])} |\n"
            )

    summary_stamp.write_text(summary_latest.read_text(encoding="utf-8"), encoding="utf-8")

    print("DOJ dataset completeness derivation complete.")
    print(f"- {counts_latest}")
    print(f"- {file_index_latest}")
    print(f"- {summary_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
