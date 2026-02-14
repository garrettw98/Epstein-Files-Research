.PHONY: help ingest-primary derive-topics claim-candidates load-db daily-report daily-pipeline

help:
	@echo "Targets:"
	@echo "  make ingest-primary  # Pull primary authority docs into raw/ and derived/"
	@echo "  make derive-topics   # Tag primary docs into topic taxonomy outputs"
	@echo "  make claim-candidates # Build pending-review claim candidates from docs"
	@echo "  make load-db         # Load latest TSV outputs into SQLite"
	@echo "  make daily-report    # Build daily primary-doc + claim-change reports"
	@echo "  make daily-pipeline  # ingest-primary + derive-topics + claim-candidates + load-db + daily-report"

ingest-primary:
	python3 scripts/ingest_primary_authority_docs.py

derive-topics:
	python3 scripts/derive_primary_doc_topics.py

claim-candidates:
	python3 scripts/generate_claim_candidates.py

load-db:
	python3 scripts/load_epstein_sqlite.py

daily-report:
	python3 scripts/generate_daily_change_report.py

daily-pipeline:
	./scripts/run_daily_pipeline.sh
