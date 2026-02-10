"""
Load cleaned sensors data into Cloud Bigtable.

Schema: Time-bucketed rows with cell versioning
  Row Key:    {sensor_id}#{YYYY-MM-DDTHH}   (UTC hour bucket)
  Families:   vitals  ==> heart_rate, body_temperature, spO2
              meta    ==> battery_level, heart_rate_imputed

Each sensor event is stored as a new cell version (keyed by its event
timestamp) inside the row for that sensor's hour bucket.  This means one
row holds all readings for one sensor in one hour — perfectly aligned
with the target query 'get all vitals for Patient X for the last 1 hour'.
"""

import json
import os
import datetime
from collections import defaultdict
from os.path import join, dirname

from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from google.cloud.bigtable import column_family

from dotenv import load_dotenv, dotenv_values
from log import log, substep

load_dotenv()

env = dotenv_values()
PROJECT_ID = env.get('PROJECT_ID', 'test-project')
INSTANCE_ID = env.get('INSTANCE_ID', 'test-instance')
TABLE_ID = env.get('TABLE_ID', 'patient_vitals')

CF_VITALS = 'vitals'
CF_META = 'meta'

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_client():
    """Return Bigtable admin client, defaulting to local emulator."""

    os.environ.setdefault('BIGTABLE_EMULATOR_HOST', env.get('BIGTABLE_EMULATOR_HOST', 'localhost:8086'))
    return bigtable.Client(project=PROJECT_ID, admin=True)


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def create_table(instance):
    """Create patient_vitals with two column families."""
    table = instance.table(TABLE_ID)

    if table.exists():
        table.delete()

    # Hot-storage window: auto-GC cells older than 7 days
    gc_rule = column_family.MaxAgeGCRule(datetime.timedelta(days=7))

    table.create(column_families={CF_VITALS: gc_rule, CF_META: gc_rule})
    return table


# ---------------------------------------------------------------------------
# Row key helpers
# ---------------------------------------------------------------------------

def make_row_key(sensor_id: str, event_timestamp_ms: int) -> str:
    """Build row key:  {sensor_id}#{YYYY-MM-DDTHH}  (UTC hour bucket)."""
    dt = datetime.datetime.fromtimestamp(
        event_timestamp_ms / 1000, tz=datetime.timezone.utc
    )
    return f'{sensor_id}#{dt:%Y-%m-%dT%H}'


def _ts_to_dt(ms: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(table, data_path: str) -> tuple[int, int]:
    """Load cleaned JSONL into Bigtable using cell versioning."""
    with open(data_path) as f:
        records = [json.loads(line) for line in f]

    # Group events by row key so each row is mutated once
    grouped = defaultdict(list)
    for rec in records:
        key = make_row_key(rec['sensor_id'], rec['event_timestamp'])
        grouped[key].append(rec)

    batch = []
    for row_key, events in grouped.items():
        row = table.direct_row(row_key)
        for evt in events:
            ts = _ts_to_dt(evt['event_timestamp'])
            row.set_cell(CF_VITALS, 'heart_rate',        str(evt['heart_rate']).encode(),        timestamp=ts)
            row.set_cell(CF_VITALS, 'body_temperature',  str(evt['body_temperature']).encode(),  timestamp=ts)
            row.set_cell(CF_VITALS, 'spO2',              str(evt['spO2']).encode(),              timestamp=ts)
            row.set_cell(CF_META,   'battery_level',     str(evt['battery_level']).encode(),     timestamp=ts)
            row.set_cell(CF_META,   'heart_rate_imputed', str(evt['heart_rate_imputed']).encode(), timestamp=ts)
        batch.append(row)

        if len(batch) >= 50:
            table.mutate_rows(batch)
            batch = []

    if batch:
        table.mutate_rows(batch)

    return len(records), len(grouped)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def query_patient_vitals(table, sensor_id: str, reference_time_ms: int):
    """Get all vitals for *sensor_id* in the 1 hour before *reference_time_ms*.

    Reads at most two hour-bucket rows and filters by cell timestamp.
    """
    ref_dt = _ts_to_dt(reference_time_ms)
    start_dt = ref_dt - datetime.timedelta(hours=1)

    # At most two buckets span a 1-hour window
    buckets = sorted({
        f'{sensor_id}#{start_dt:%Y-%m-%dT%H}',
        f'{sensor_id}#{ref_dt:%Y-%m-%dT%H}',
    })

    ts_range = row_filters.TimestampRange(start=start_dt, end=ref_dt)
    ts_filter = row_filters.TimestampRangeFilter(ts_range)

    results = []
    for rk in buckets:
        row = table.read_row(rk, filter_=ts_filter)
        if row is None:
            continue

        vitals = row.cells.get(CF_VITALS, {})
        hr_cells   = vitals.get(b'heart_rate', [])
        bt_cells   = vitals.get(b'body_temperature', [])
        spo2_cells = vitals.get(b'spO2', [])

        for i, cell in enumerate(hr_cells):
            results.append({
                'timestamp':        cell.timestamp.isoformat(),
                'heart_rate':       float(cell.value.decode()),
                'body_temperature': float(bt_cells[i].value.decode()) if i < len(bt_cells) else None,
                'spO2':             int(spo2_cells[i].value.decode()) if i < len(spo2_cells) else None,
            })

    results.sort(key=lambda r: r['timestamp'])
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    log('Connecting to Bigtable emulator ...')
    client = get_client()
    instance = client.instance(INSTANCE_ID)

    log('Creating table ...')
    table = create_table(instance)
    substep(1, f'Created table "{TABLE_ID}" [{CF_VITALS}, {CF_META}]')

    log('Loading cleaned data ...')
    data_file = join(dirname(__file__), '..', 'data', 'vitals_cleaned.jsonl')
    event_count, row_count = load_data(table, data_file)
    substep(2, f'{event_count} events ==> {row_count} rows')

    # --- demo query ---
    with open(data_file) as f:
        first = json.loads(f.readline())

    demo_sensor = first['sensor_id']
    demo_ref = first['event_timestamp'] + 3_600_000  # 1 h after first event

    log(f'Demo query: vitals for "{demo_sensor}", 1 h before {_ts_to_dt(demo_ref).isoformat()}')
    rows = query_patient_vitals(table, demo_sensor, demo_ref)
    substep(3, f'{len(rows)} readings returned')
    for r in rows[:5]:
        substep(4, f'{r["timestamp"]}  hr={r["heart_rate"]}  bt={r["body_temperature"]}  spO2={r["spO2"]}')
    if len(rows) > 5:
        substep(5, f'... and {len(rows) - 5} more')
