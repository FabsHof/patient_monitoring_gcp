.PHONY: ingest_data run_emulator bigtable_load bigquery_load vertex_pipeline all

ingest_data:
	@python code/ingest.py

run_emulator:
	@docker compose up -d

bigtable_load:
	@python code/bigtable_load.py

bigquery_load:
	@python code/bigquery_load.py

vertex_pipeline:
	@python code/vertex_pipeline.py

all: ingest_data run_emulator bigtable_load bigquery_load vertex_pipeline
