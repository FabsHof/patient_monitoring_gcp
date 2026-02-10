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

    # Parse the timestamp column to datetime, filtering out invalid entries
    data['event_timestamp'] = pd.to_datetime(data['event_timestamp'], errors='coerce')
    data = data[~data['event_timestamp'].isna()]

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
