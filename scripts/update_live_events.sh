#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
README_FILE="${ROOT_DIR}/README.md"
EVIDENCE_FILE="${ROOT_DIR}/evidence/2026_Release.md"
TIMELINE_FILE="${ROOT_DIR}/timeline/Full_Timeline.md"
DOJ_URL="https://www.justice.gov/epstein"

AS_OF=""
SCOPE_END=""
DATASET=""
EVENTS_FILE=""
EVENTS_PATH=""
COMMIT_CHANGES="false"
PUSH_CHANGES="false"
COMMIT_MESSAGE=""

EVENT_LINES=()
EVENT_DATES=()
EVENT_TITLES=()
EVENT_DETAILS=()
EVENT_URLS=()

usage() {
  cat <<'EOF'
Usage:
  scripts/update_live_events.sh --as-of "Feb 13, 2026" [options]

Required:
  --as-of "Mon DD, YYYY"          As-of date shown in live update sections.

Event input (choose at least one):
  --events-file PATH              File with one event per line:
                                  DATE|TITLE|DETAIL|URL
  --event "DATE|TITLE|DETAIL|URL" Inline event (repeatable).

Optional:
  --scope-end "Mon DD, YYYY"      Timeline scope end date (defaults to --as-of).
  --dataset N                     DOJ data set number (example: 12).
  --doj-url URL                   DOJ source URL (default: https://www.justice.gov/epstein).
  --commit                        Commit updated files.
  --push                          Push commit to origin/<current-branch> (requires --commit).
  --message "text"                Commit message override.
  -h, --help                      Show this help.

Examples:
  scripts/update_live_events.sh \
    --as-of "Feb 13, 2026" \
    --dataset 12 \
    --events-file updates/live_events.latest.txt

  scripts/update_live_events.sh \
    --as-of "Feb 14, 2026" \
    --dataset 13 \
    --event "2026 (Feb 14)|Example headline|Short verified detail sentence.|https://example.com" \
    --commit --push
EOF
}

fail() {
  echo "Error: $*" >&2
  exit 1
}

require_file() {
  local file="$1"
  [[ -f "${file}" ]] || fail "Required file not found: ${file}"
}

parse_event_line() {
  local line="$1"
  local date_part=""
  local title_part=""
  local detail_part=""
  local url_part=""
  local extra_part=""

  IFS='|' read -r date_part title_part detail_part url_part extra_part <<EOF
${line}
EOF

  [[ -n "${date_part}" ]] || fail "Event line missing DATE: ${line}"
  [[ -n "${title_part}" ]] || fail "Event line missing TITLE: ${line}"
  [[ -n "${detail_part}" ]] || fail "Event line missing DETAIL: ${line}"
  [[ -n "${url_part}" ]] || fail "Event line missing URL: ${line}"
  [[ -z "${extra_part}" ]] || fail "Event line has extra fields (expect DATE|TITLE|DETAIL|URL): ${line}"
  [[ "${url_part}" =~ ^https?:// ]] || fail "Event URL must start with http:// or https://: ${url_part}"

  EVENT_DATES+=("${date_part}")
  EVENT_TITLES+=("${title_part}")
  EVENT_DETAILS+=("${detail_part}")
  EVENT_URLS+=("${url_part}")
}

replace_managed_block() {
  local file="$1"
  local start_marker="$2"
  local end_marker="$3"
  local block_file="$4"
  local tmp_file

  tmp_file="$(mktemp)"
  awk -v start="${start_marker}" -v end="${end_marker}" -v block_file="${block_file}" '
    BEGIN {
      while ((getline line < block_file) > 0) {
        block = block line ORS
      }
      close(block_file)
    }
    $0 == start {
      printf "%s", block
      in_block = 1
      replaced = 1
      next
    }
    $0 == end {
      in_block = 0
      next
    }
    !in_block {
      print
    }
    END {
      if (!replaced) {
        exit 2
      }
    }
  ' "${file}" > "${tmp_file}" || {
    rm -f "${tmp_file}"
    fail "Could not replace managed block in ${file}; check markers ${start_marker} / ${end_marker}"
  }

  mv "${tmp_file}" "${file}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --as-of)
      AS_OF="${2:-}"
      shift 2
      ;;
    --scope-end)
      SCOPE_END="${2:-}"
      shift 2
      ;;
    --dataset)
      DATASET="${2:-}"
      shift 2
      ;;
    --doj-url)
      DOJ_URL="${2:-}"
      shift 2
      ;;
    --events-file)
      EVENTS_FILE="${2:-}"
      shift 2
      ;;
    --event)
      EVENT_LINES+=("${2:-}")
      shift 2
      ;;
    --commit)
      COMMIT_CHANGES="true"
      shift
      ;;
    --push)
      PUSH_CHANGES="true"
      shift
      ;;
    --message)
      COMMIT_MESSAGE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "Unknown argument: $1"
      ;;
  esac
done

[[ -n "${AS_OF}" ]] || fail "--as-of is required"
[[ -n "${SCOPE_END}" ]] || SCOPE_END="${AS_OF}"
[[ "${PUSH_CHANGES}" == "false" || "${COMMIT_CHANGES}" == "true" ]] || fail "--push requires --commit"

require_file "${README_FILE}"
require_file "${EVIDENCE_FILE}"
require_file "${TIMELINE_FILE}"

if [[ -n "${EVENTS_FILE}" ]]; then
  if [[ "${EVENTS_FILE}" == /* ]]; then
    EVENTS_PATH="${EVENTS_FILE}"
  else
    EVENTS_PATH="${ROOT_DIR}/${EVENTS_FILE}"
  fi
  require_file "${EVENTS_PATH}"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -z "${line}" ]] && continue
    [[ "${line}" =~ ^# ]] && continue
    EVENT_LINES+=("${line}")
  done < "${EVENTS_PATH}"
fi

[[ ${#EVENT_LINES[@]} -gt 0 ]] || fail "No events supplied. Use --event and/or --events-file."

for event_line in "${EVENT_LINES[@]}"; do
  parse_event_line "${event_line}"
done

AS_OF="${AS_OF}" perl -0pi -e 's/^## The 2026 Release \(Updated [^)]+\)$/## The 2026 Release (Updated $ENV{AS_OF})/m' "${README_FILE}"
SCOPE_END="${SCOPE_END}" perl -0pi -e 's/^> \*\*Scope\*\*: 1953 - .+$/> **Scope**: 1953 - $ENV{SCOPE_END}/m' "${TIMELINE_FILE}"

readme_block="$(mktemp)"
{
  echo "<!-- LIVE_UPDATES:START -->"
  echo "### Live Updates (As of ${AS_OF})"
  for idx in "${!EVENT_DATES[@]}"; do
    printf '*   **%s** (%s): %s [Source](%s)\n' \
      "${EVENT_TITLES[$idx]}" "${EVENT_DATES[$idx]}" "${EVENT_DETAILS[$idx]}" "${EVENT_URLS[$idx]}"
  done
  if [[ -n "${DATASET}" ]]; then
    printf "*   **DOJ library status (%s)**: DOJ's public Epstein repository remains live and currently lists releases through **Data Set %s**. [DOJ Epstein Library](%s)\n" \
      "${AS_OF}" "${DATASET}" "${DOJ_URL}"
  else
    printf "*   **DOJ library status (%s)**: DOJ's public Epstein repository remains live. [DOJ Epstein Library](%s)\n" \
      "${AS_OF}" "${DOJ_URL}"
  fi
  echo "<!-- LIVE_UPDATES:END -->"
} > "${readme_block}"

evidence_block="$(mktemp)"
{
  echo "<!-- REALTIME_STATUS:START -->"
  echo "### Real-Time Status (As of ${AS_OF})"
  echo
  if [[ -n "${DATASET}" ]]; then
    printf -- "- DOJ's public repository remains online and publicly lists releases through **Data Set %s**.\n" "${DATASET}"
  else
    echo "- DOJ's public repository remains online; verify the latest listed data set on the official page."
  fi
  printf -- "- Source: [DOJ Epstein Library](%s)\n" "${DOJ_URL}"
  echo
  echo "### Confirmed Fallout Updates"
  echo
  for idx in "${!EVENT_DATES[@]}"; do
    printf -- "- **%s**: **%s**. %s [Source](%s)\n" \
      "${EVENT_DATES[$idx]}" "${EVENT_TITLES[$idx]}" "${EVENT_DETAILS[$idx]}" "${EVENT_URLS[$idx]}"
  done
  echo "<!-- REALTIME_STATUS:END -->"
} > "${evidence_block}"

timeline_block="$(mktemp)"
{
  echo "<!-- LIVE_TIMELINE:START -->"
  for idx in "${!EVENT_DATES[@]}"; do
    printf '*   **%s**: **%s**.\n' "${EVENT_DATES[$idx]}" "${EVENT_TITLES[$idx]}"
    printf '    *   %s\n' "${EVENT_DETAILS[$idx]}"
    printf '    *   Source: [Source](%s).\n' "${EVENT_URLS[$idx]}"
  done
  echo "<!-- LIVE_TIMELINE:END -->"
} > "${timeline_block}"

replace_managed_block "${README_FILE}" "<!-- LIVE_UPDATES:START -->" "<!-- LIVE_UPDATES:END -->" "${readme_block}"
replace_managed_block "${EVIDENCE_FILE}" "<!-- REALTIME_STATUS:START -->" "<!-- REALTIME_STATUS:END -->" "${evidence_block}"
replace_managed_block "${TIMELINE_FILE}" "<!-- LIVE_TIMELINE:START -->" "<!-- LIVE_TIMELINE:END -->" "${timeline_block}"

rm -f "${readme_block}" "${evidence_block}" "${timeline_block}"

echo "Updated managed live sections:"
echo "- ${README_FILE}"
echo "- ${EVIDENCE_FILE}"
echo "- ${TIMELINE_FILE}"

if [[ "${COMMIT_CHANGES}" == "true" ]]; then
  branch_name="$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD)"
  git -C "${ROOT_DIR}" add README.md evidence/2026_Release.md timeline/Full_Timeline.md
  if [[ -n "${EVENTS_PATH}" && "${EVENTS_PATH}" == "${ROOT_DIR}/"* ]]; then
    git -C "${ROOT_DIR}" add "${EVENTS_PATH#${ROOT_DIR}/}"
  fi
  if [[ -z "${COMMIT_MESSAGE}" ]]; then
    COMMIT_MESSAGE="Update live events as of ${AS_OF}"
  fi
  git -C "${ROOT_DIR}" commit -m "${COMMIT_MESSAGE}"
  echo "Committed on ${branch_name}: ${COMMIT_MESSAGE}"
fi

if [[ "${PUSH_CHANGES}" == "true" ]]; then
  branch_name="$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD)"
  git -C "${ROOT_DIR}" push origin "${branch_name}"
  echo "Pushed to origin/${branch_name}"
fi
