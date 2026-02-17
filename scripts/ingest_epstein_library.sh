#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_URL="https://www.justice.gov/epstein"
DISCLOSURES_URL="https://www.justice.gov/epstein/doj-disclosures"
RAW_DIR="${ROOT_DIR}/raw/doj_epstein_library"
DERIVED_DIR="${ROOT_DIR}/derived/doj_epstein_library"
SEED_FILE="${ROOT_DIR}/evidence/Primary_Sources_Index.md"
CHECK_STATUS="true"
MAX_URLS=""

usage() {
  cat <<'EOF'
Usage:
  scripts/ingest_epstein_library.sh [options]

Options:
  --source-url URL      Source page to ingest (default: https://www.justice.gov/epstein)
  --disclosures-url URL Optional disclosures page URL for dataset discovery fallback
                        (default: https://www.justice.gov/epstein/doj-disclosures)
  --raw-dir PATH        Raw snapshot directory (default: raw/doj_epstein_library)
  --derived-dir PATH    Derived output directory (default: derived/doj_epstein_library)
  --seed-file PATH      Optional markdown/txt file with seed URLs to include
  --no-status-check     Skip per-link HTTP status checks (faster)
  --max-urls N          Optional cap on processed URLs (for quick tests)
  -h, --help            Show help

Outputs:
  raw/doj_epstein_library/<timestamp>_epstein.html
  derived/doj_epstein_library/epstein_library_index_latest.tsv
  derived/doj_epstein_library/epstein_library_index_<timestamp>.tsv
  derived/doj_epstein_library/epstein_library_summary_latest.md
EOF
}

fail() {
  echo "Error: $*" >&2
  exit 1
}

classify_link() {
  local url="$1"
  local lower
  lower="$(printf '%s' "$url" | tr '[:upper:]' '[:lower:]')"

  if [[ "$lower" =~ \.pdf($|[?#]) ]]; then
    echo "pdf"
  elif [[ "$lower" =~ \.(zip|7z|gz|tgz|csv|tsv|xlsx?|json|xml)($|[?#]) ]]; then
    echo "data_file"
  elif [[ "$lower" =~ \.(jpg|jpeg|png|gif|webp|tiff?)($|[?#]) ]]; then
    echo "image"
  elif [[ "$lower" =~ \.(mp4|mov|avi|mkv|m4v|webm)($|[?#]) ]]; then
    echo "video"
  elif [[ "$lower" =~ dataset|data-set|set[[:space:]_-]*[0-9]+|/download/|/library/ ]]; then
    echo "dataset_or_download"
  elif [[ "$lower" =~ /epstein ]]; then
    echo "epstein_page"
  else
    echo "other"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-url)
      SOURCE_URL="${2:-}"
      shift 2
      ;;
    --disclosures-url)
      DISCLOSURES_URL="${2:-}"
      shift 2
      ;;
    --raw-dir)
      RAW_DIR="${2:-}"
      shift 2
      ;;
    --derived-dir)
      DERIVED_DIR="${2:-}"
      shift 2
      ;;
    --seed-file)
      SEED_FILE="${2:-}"
      shift 2
      ;;
    --no-status-check)
      CHECK_STATUS="false"
      shift
      ;;
    --max-urls)
      MAX_URLS="${2:-}"
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

[[ -n "${SOURCE_URL}" ]] || fail "source URL is required"

mkdir -p "${RAW_DIR}" "${DERIVED_DIR}"

TIMESTAMP_UTC="$(date -u +"%Y%m%dT%H%M%SZ")"
SNAPSHOT_HTML="${RAW_DIR}/${TIMESTAMP_UTC}_epstein.html"
DISCLOSURES_SNAPSHOT_HTML="${RAW_DIR}/${TIMESTAMP_UTC}_epstein_doj_disclosures.html"
INDEX_LATEST="${DERIVED_DIR}/epstein_library_index_latest.tsv"
INDEX_TIMESTAMPED="${DERIVED_DIR}/epstein_library_index_${TIMESTAMP_UTC}.tsv"
SUMMARY_LATEST="${DERIVED_DIR}/epstein_library_summary_latest.md"

TMP_HREFS="$(mktemp)"
TMP_NORMALIZED="$(mktemp)"
TMP_FILTERED="$(mktemp)"

cleanup() {
  rm -f "${TMP_HREFS}" "${TMP_NORMALIZED}" "${TMP_FILTERED}"
}
trap cleanup EXIT

curl -A "Mozilla/5.0" -fsSL --retry 3 --connect-timeout 20 --max-time 120 \
  "${SOURCE_URL}" -o "${SNAPSHOT_HTML}"

grep -Eo "href=['\"][^'\"]+['\"]" "${SNAPSHOT_HTML}" \
  | sed -E "s/^href=['\"]//; s/['\"]$//" \
  | sed 's/&amp;/\&/g' \
  | sort -u > "${TMP_HREFS}"

# Fallback extraction for pages that render links via scripts/JSON blobs.
if [[ "$(wc -l < "${TMP_HREFS}")" -lt 5 ]]; then
  grep -Eo "https?://[^\"' <>]+" "${SNAPSHOT_HTML}" >> "${TMP_HREFS}" || true
  grep -Eo "/epstein[^\"' <>]+" "${SNAPSHOT_HTML}" >> "${TMP_HREFS}" || true
  sort -u -o "${TMP_HREFS}" "${TMP_HREFS}"
fi

HAS_DISCLOSURES_SNAPSHOT="false"
if [[ -n "${DISCLOSURES_URL}" ]]; then
  if curl -A "Mozilla/5.0" -fsSL --retry 3 --connect-timeout 20 --max-time 120 \
    "${DISCLOSURES_URL}" -o "${DISCLOSURES_SNAPSHOT_HTML}"; then
    HAS_DISCLOSURES_SNAPSHOT="true"
    grep -Eo "href=['\"][^'\"]+['\"]" "${DISCLOSURES_SNAPSHOT_HTML}" \
      | sed -E "s/^href=['\"]//; s/['\"]$//" \
      | sed 's/&amp;/\&/g' \
      >> "${TMP_HREFS}"
    sort -u -o "${TMP_HREFS}" "${TMP_HREFS}"
  fi
fi

SOURCE_PREFIX="${SOURCE_URL%/}"
SOURCE_DIR="${SOURCE_PREFIX%/*}"

while IFS= read -r link; do
  [[ -z "${link}" ]] && continue
  [[ "${link}" =~ ^# ]] && continue
  [[ "${link}" =~ ^mailto: ]] && continue
  [[ "${link}" =~ ^javascript: ]] && continue
  [[ "${link}" =~ (facebook\.com/sharer|twitter\.com/intent/tweet|linkedin\.com/shareArticle) ]] && continue

  if [[ "${link}" =~ ^https?:// ]]; then
    norm="${link}"
  elif [[ "${link}" =~ ^// ]]; then
    norm="https:${link}"
  elif [[ "${link}" =~ ^/ ]]; then
    norm="https://www.justice.gov${link}"
  else
    norm="${SOURCE_DIR}/${link}"
  fi

  # Drop transient bot/marketing parameters that create unstable URLs.
  norm="$(printf '%s' "${norm}" | sed -E \
    -e 's/([?&])bm-verify=[^&]*(&|$)/\1/g' \
    -e 's/([?&])utm_[^=]*=[^&]*(&|$)/\1/g' \
    -e 's/([?&])output=amp(&|$)/\1/g' \
    -e 's/[?&]+$//' \
    -e 's/\?&/\?/g' \
    -e 's/\?\?+/\?/g')"

  [[ -n "${norm}" ]] && echo "${norm}" >> "${TMP_NORMALIZED}"
done < "${TMP_HREFS}"

sort -u "${TMP_NORMALIZED}" \
  | grep -Ei 'epstein|dataset|data-set|set[[:space:]_-]*[0-9]+|efta|\.pdf($|[?#])|\.zip($|[?#])|\.csv($|[?#])|\.json($|[?#])|/download/' \
  > "${TMP_FILTERED}" || true

if [[ "$(wc -l < "${TMP_FILTERED}")" -lt 4 ]]; then
  {
    echo "${SOURCE_URL}"
    echo "${SOURCE_URL}#court-records"
    echo "https://www.justice.gov/opa/pr/attorney-general-pamela-bondi-releases-first-phase-declassified-epstein-files"
    echo "https://www.justice.gov/opa/pr/department-justice-publishes-35-million-responsive-pages-compliance-epstein-files"
  } | sort -u > "${TMP_FILTERED}"
fi

if [[ -n "${SEED_FILE}" && -f "${SEED_FILE}" ]]; then
  rg --no-filename -o 'https?://[^) ]+' "${SEED_FILE}" | sed 's/[.,]$//' >> "${TMP_FILTERED}" || true
  sort -u -o "${TMP_FILTERED}" "${TMP_FILTERED}"
fi

if [[ -n "${MAX_URLS}" ]]; then
  head -n "${MAX_URLS}" "${TMP_FILTERED}" > "${TMP_FILTERED}.capped"
  mv "${TMP_FILTERED}.capped" "${TMP_FILTERED}"
fi

printf "ingested_at_utc\tsource_page\tsnapshot_file\turl\tpath\tlink_type\tstatus_code\n" > "${INDEX_LATEST}"

while IFS= read -r url; do
  [[ -z "${url}" ]] && continue

  path="$(printf '%s' "${url}" | sed -E 's#https?://[^/]+##')"
  [[ -n "${path}" ]] || path="/"
  link_type="$(classify_link "${url}")"

  if [[ "${CHECK_STATUS}" == "true" ]]; then
    status_code="$(curl -A "Mozilla/5.0" -I -L -s -o /dev/null -w "%{http_code}" "${url}" || true)"
    [[ -n "${status_code}" ]] || status_code="000"
  else
    status_code="unchecked"
  fi

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "${TIMESTAMP_UTC}" \
    "${SOURCE_URL}" \
    "${SNAPSHOT_HTML#${ROOT_DIR}/}" \
    "${url}" \
    "${path}" \
    "${link_type}" \
    "${status_code}" >> "${INDEX_LATEST}"
done < "${TMP_FILTERED}"

cp "${INDEX_LATEST}" "${INDEX_TIMESTAMPED}"

{
  echo "# Epstein Library Ingest Summary"
  echo
  echo "- Run UTC: ${TIMESTAMP_UTC}"
  echo "- Source page: ${SOURCE_URL}"
  if [[ "${HAS_DISCLOSURES_SNAPSHOT}" == "true" ]]; then
    echo "- Disclosures page: ${DISCLOSURES_URL}"
  fi
  echo "- Raw snapshot: ${SNAPSHOT_HTML#${ROOT_DIR}/}"
  if [[ "${HAS_DISCLOSURES_SNAPSHOT}" == "true" ]]; then
    echo "- Disclosures snapshot: ${DISCLOSURES_SNAPSHOT_HTML#${ROOT_DIR}/}"
  fi
  echo "- Index latest: ${INDEX_LATEST#${ROOT_DIR}/}"
  echo "- Index snapshot: ${INDEX_TIMESTAMPED#${ROOT_DIR}/}"
  echo "- URL count: $(( $(wc -l < "${INDEX_LATEST}") - 1 ))"
  echo
  echo "## By Link Type"
  awk -F '\t' 'NR>1 {c[$6]++} END{for(k in c) printf "- %s: %d\n", k, c[k]}' "${INDEX_LATEST}" | sort
  echo
  echo "## By HTTP Status"
  awk -F '\t' 'NR>1 {c[$7]++} END{for(k in c) printf "- %s: %d\n", k, c[k]}' "${INDEX_LATEST}" | sort -nr
} > "${SUMMARY_LATEST}"

echo "Ingest complete."
echo "- ${SNAPSHOT_HTML#${ROOT_DIR}/}"
if [[ "${HAS_DISCLOSURES_SNAPSHOT}" == "true" ]]; then
  echo "- ${DISCLOSURES_SNAPSHOT_HTML#${ROOT_DIR}/}"
fi
echo "- ${INDEX_LATEST#${ROOT_DIR}/}"
echo "- ${INDEX_TIMESTAMPED#${ROOT_DIR}/}"
echo "- ${SUMMARY_LATEST#${ROOT_DIR}/}"
