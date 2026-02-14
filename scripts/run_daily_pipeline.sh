#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/ingest_primary_authority_docs.py "$@"
python3 scripts/derive_primary_doc_topics.py
python3 scripts/generate_claim_candidates.py
python3 scripts/load_epstein_sqlite.py
python3 scripts/generate_daily_change_report.py

echo "Daily pipeline complete."
