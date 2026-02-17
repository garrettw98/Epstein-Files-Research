#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

./scripts/ingest_epstein_library.sh
python3 scripts/derive_doj_dataset_completeness.py
python3 scripts/ingest_primary_authority_docs.py "$@"
python3 scripts/derive_primary_doc_topics.py
python3 scripts/generate_claim_candidates.py
python3 scripts/derive_entity_mentions.py
python3 scripts/assess_claim_context_quality.py
python3 scripts/triage_claim_quality_flags.py
python3 scripts/load_epstein_sqlite.py
python3 scripts/generate_daily_change_report.py
python3 scripts/generate_coverage_gap_dashboard.py
python3 scripts/update_last24h_brief.py

echo "Daily pipeline complete."
