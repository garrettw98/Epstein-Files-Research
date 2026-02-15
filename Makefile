.PHONY: help ingest-primary derive-topics claim-candidates derive-entities claim-quality coverage-gaps load-db daily-report daily-pipeline test

help:
	@echo "Targets:"
	@echo "  make ingest-primary  # Pull primary authority docs into raw/ and derived/"
	@echo "  make derive-topics   # Tag primary docs into topic taxonomy outputs"
	@echo "  make claim-candidates # Build pending-review claim candidates from docs"
	@echo "  make derive-entities # Build canonical aliases + context-typed entity mentions"
	@echo "  make claim-quality   # Flag name-only and weak-context claim risks"
	@echo "  make coverage-gaps   # Build dataset/source health dashboard"
	@echo "  make load-db         # Load latest TSV outputs into SQLite"
	@echo "  make daily-report    # Build daily primary-doc + claim-change reports"
	@echo "  make daily-pipeline  # full ingest/derive/quality/load/report pipeline"
	@echo "  make test            # Run script unit tests"

ingest-primary:
	python3 scripts/ingest_primary_authority_docs.py

derive-topics:
	python3 scripts/derive_primary_doc_topics.py

claim-candidates:
	python3 scripts/generate_claim_candidates.py

derive-entities:
	python3 scripts/derive_entity_mentions.py

claim-quality:
	python3 scripts/assess_claim_context_quality.py

coverage-gaps:
	python3 scripts/generate_coverage_gap_dashboard.py

load-db:
	python3 scripts/load_epstein_sqlite.py

daily-report:
	python3 scripts/generate_daily_change_report.py

daily-pipeline:
	./scripts/run_daily_pipeline.sh

test:
	python3 -m unittest discover -s tests -p 'test_*.py'
