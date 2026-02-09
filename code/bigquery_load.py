"""
Load cleaned sensor data into BigQuery for historical analytics.

Target table schema (flat, one row per reading):
    event_timestamp     TIMESTAMP   (partitioned by DAY)
    sensor_id           STRING      (clustered)
    heart_rate          FLOAT64
    body_temperature    FLOAT64
    spO2                INT64
    battery_level       INT64
    heart_rate_imputed  BOOL

Partitioning on event_timestamp (DAY) and clustering on sensor_id
are applied at table creation time so analytical queries only scan
the date ranges and sensors they actually need.
"""

import json
import datetime
from os.path import join, dirname

from google.cloud import bigquery
from dotenv import load_dotenv, dotenv_values

load_dotenv()

env = dotenv_values()
PROJECT_ID = env.get('PROJECT_ID', 'patient-monitoring-dev')
BQ_DATASET = env.get('BQ_DATASET', 'patient_monitoring')
BQ_TABLE = env.get('BQ_TABLE', 'patient_vitals')


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = [
    bigquery.SchemaField('event_timestamp',    'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('sensor_id',          'STRING',    mode='REQUIRED'),
    bigquery.SchemaField('heart_rate',         'FLOAT64',   mode='REQUIRED'),
    bigquery.SchemaField('body_temperature',   'FLOAT64',   mode='REQUIRED'),
    bigquery.SchemaField('spO2',               'INT64',     mode='REQUIRED'),
    bigquery.SchemaField('battery_level',      'INT64',     mode='REQUIRED'),
    bigquery.SchemaField('heart_rate_imputed', 'BOOL',      mode='REQUIRED'),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ms_to_iso(ms: int) -> str:
    """Convert Unix-millisecond timestamp to ISO-8601 string (UTC)."""
    dt = datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)
    return dt.isoformat()


def _read_jsonl(path: str) -> list[dict]:
    """Read JSONL file and convert timestamps for BigQuery."""
    rows = []
    with open(path) as f:
        for line in f:
            rec = json.loads(line)
            rec['event_timestamp'] = _ms_to_iso(rec['event_timestamp'])
            rows.append(rec)
    return rows


# ---------------------------------------------------------------------------
# Dataset & table creation
# ---------------------------------------------------------------------------

def ensure_dataset(client: bigquery.Client) -> bigquery.Dataset:
    """Create the dataset if it does not exist yet."""
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, BQ_DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f'Dataset "{BQ_DATASET}" ready.')
    return dataset


def create_table(client: bigquery.Client) -> bigquery.Table:
    """Create (or replace) the partitioned + clustered vitals table."""
    table_ref = f'{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}'
    table = bigquery.Table(table_ref, schema=SCHEMA)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='event_timestamp',
    )
    table.clustering_fields = ['sensor_id']

    client.delete_table(table_ref, not_found_ok=True)
    table = client.create_table(table)
    print(f'Created table "{table_ref}" (partitioned by DAY, clustered by sensor_id).')
    return table


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(client: bigquery.Client, table: bigquery.Table, rows: list[dict]) -> int:
    """Load rows into BigQuery using the streaming-friendly JSON loader."""
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    job = client.load_table_from_json(rows, table, job_config=job_config)
    job.result()  # block until complete

    dest = client.get_table(table)
    print(f'Loaded {dest.num_rows} rows into {dest.full_table_id}.')
    return dest.num_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    print('1) Connecting to BigQuery ...')
    client = bigquery.Client(project=PROJECT_ID)

    print('2) Ensuring dataset exists ...')
    ensure_dataset(client)

    print('3) Creating table ...')
    table = create_table(client)

    print('4) Reading cleaned data ...')
    data_file = join(dirname(__file__), '..', 'data', 'vitals_cleaned.jsonl')
    rows = _read_jsonl(data_file)
    print(f'   {len(rows)} records read.')

    print('5) Loading into BigQuery ...')
    load_data(client, table, rows)

    print('\nDone. Run analytical queries in the BigQuery console or via')
    print(f'  bq query --use_legacy_sql=false < sql/analytical_query.sql')
