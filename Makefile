.PHONY: ingest_data run_emulator bigtable_load

ingest_data:
	@echo "Starting data ingestion and cleaning process..."
	@python code/ingest.py
	@echo "Data ingestion and cleaning completed successfully."

run_emulator:
	@echo "Starting Bigtable emulator..."
	@docker compose up -d
	@echo "Bigtable emulator is running."

bigtable_load:
	@echo "Loading cleaned data into Bigtable emulator..."
	@python code/bigtable_load.py
	@echo "Bigtable load completed."