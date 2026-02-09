.PHONY: ingest_data

ingest_data:
	@echo "Starting data ingestion and cleaning process..."
	@python code/ingest.py
	@echo "Data ingestion and cleaning completed successfully."