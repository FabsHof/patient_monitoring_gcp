# Ingestion And Cleaning Logic

The ingestion script reads line-delimited JSON from `data/vitals_raw.txt` and applies a minimal, clinically-aware cleaning pass before writing `data/vitals_cleaned.jsonl`.

## Cleaning Steps

- Parse `event_timestamp` with `errors='coerce'` and drop any rows that become `NaT`.
- Trim any `body_temperature` values out of the plausible human range (27-42.6°C) to the nearest bound.
- For missing `heart_rate`, impute per `sensor_id` using a rolling mean (window=3, `min_periods=1`) after sorting by `event_timestamp`.
- Keep a boolean flag `heart_rate_imputed` to preserve data lineage.
- If a `heart_rate` remains missing after imputation (e.g., all values missing for a sensor), drop the row.

## Drop vs Impute Null values

As heart rate values are often analyzed as continous time series, dropping nulls would break analytics. Therefore, imputation was included using a rolling window per sensor. For data lineage, `heart_rate_imputed` is set to `True` for these values.

