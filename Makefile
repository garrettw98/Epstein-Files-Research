.PHONY: help ingest-primary load-db daily-report daily-pipeline

help:
	@echo "Targets:"
	@echo "  make ingest-primary  # Pull primary authority docs into raw/ and derived/"
	@echo "  make load-db         # Load latest TSV outputs into SQLite"
	@echo "  make daily-report    # Build daily primary-doc + claim-change reports"
	@echo "  make daily-pipeline  # ingest-primary + load-db + daily-report"

ingest-primary:
	python3 scripts/ingest_primary_authority_docs.py

load-db:
	python3 scripts/load_epstein_sqlite.py

daily-report:
	python3 scripts/generate_daily_change_report.py

daily-pipeline:
	./scripts/run_daily_pipeline.sh
