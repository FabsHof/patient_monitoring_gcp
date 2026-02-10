.PHONY: ingest_data run_emulator load_bigtable load_bigquery vertex_pipeline all

ingest_data:
	@python code/ingest.py

run_emulator:
	@docker compose up -d

load_bigtable:
	@python code/bigtable_load.py

load_bigquery:
	@python code/bigquery_load.py

vertex_pipeline:
	@python code/vertex_pipeline.py

all: ingest_data run_emulator load_bigtable load_bigquery vertex_pipeline
