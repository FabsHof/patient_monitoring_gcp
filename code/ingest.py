import pandas as pd
import os
from os.path import join, dirname

from log import log, substep


def ingest_and_clean_data(file_path=None):
    """
    Ingests raw data from a specified file path, performs cleaning operations, and saves the cleaned dataset.
    Args:
        file_path (str): Path to the raw data file.
    Returns:
        tuple: (output_path, row_count, imputed_count)
    """
    data_path = file_path
    if data_path is None or not os.path.isfile(data_path):
        raise ValueError(f'Please provide a valid file path for the raw data. Given: {data_path}')

    # Load the dataset
    data = pd.read_json(data_path, lines=True)

    # Parse the timestamp column to datetime
    ts = data['event_timestamp']
    if pd.api.types.is_numeric_dtype(ts):
        parsed_ts = pd.to_datetime(ts, unit='ms', errors='coerce', utc=True)
    else:
        parsed_ts = pd.to_datetime(ts, errors='coerce', utc=True)

    # Filter out invalid and future timestamps
    now_utc = pd.Timestamp.now('UTC')
    parsed_ts = parsed_ts[~parsed_ts.isna()]
    parsed_ts = parsed_ts[parsed_ts <= now_utc]
    data = data.loc[parsed_ts.index]

    # Normalize to Unix milliseconds for downstream loaders
    ts_unit = getattr(parsed_ts.dtype, 'unit', 'ns')
    unit_to_divisor = {'ns': 1_000_000, 'us': 1_000, 'ms': 1}
    divisor = unit_to_divisor.get(ts_unit, 1_000_000)
    data['event_timestamp'] = (parsed_ts.astype('int64') // divisor).astype('int64')

    # Clamp body temperature values to a reasonable range (27-42.6 degrees Celsius)
    data['body_temperature'] = data['body_temperature'].clip(lower=27, upper=42.6)

    # Handle missing heart rate values (e.g., fill with rolling average per sensor, marking imputed rows)
    data['heart_rate_imputed'] = data['heart_rate'].isna()
    data = data.sort_values(['sensor_id', 'event_timestamp'])
    data['heart_rate'] = (
        data
        .groupby('sensor_id')['heart_rate']
        .transform(lambda x: x.fillna(x.rolling(window=3, min_periods=1).mean()))
    )
    data = data[~data['heart_rate'].isna()]

    # Save cleaned output
    output_path = join(dirname(data_path), 'vitals_cleaned.jsonl')
    data.to_json(output_path, orient='records', lines=True)

    return output_path, len(data), int(data['heart_rate_imputed'].sum())


if __name__ == '__main__':
    log('Ingesting and cleaning raw data ...')
    file_path = join('data', 'vitals_raw.txt')
    output_path, row_count, imputed_count = ingest_and_clean_data(file_path)

    substep(1, f'Cleaned rows: {row_count}')
    substep(2, f'Imputed heart rate rows: {imputed_count}')
    substep(3, f'Output path: {output_path}')
