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

# Schema Defense

The schema design for the Bigtable hot storage layer is optimized for the primary query pattern: **"Get all vitals for Patient X for the last 1 hour."**

Therefore, the row key is structured as `{sensor_id}#{YYYY-MM-DDTHH}` (time bucket), which allows efficient retrieval of all events for a sensor within a specific hour. Due to this, data is compressed into time buckets, and cell versioning is used to store multiple events within the same hour without creating new rows or columns. This design ensures low-latency lookups while keeping the schema simple and efficient for the intended access patterns.

| Property | Value |
| --- | --- |
| Table | `patient_vitals` |
| Row Key | `{sensor_id}#{YYYY-MM-DDTHH}` (time bucket) |
| Column Family **vitals** | `heart_rate`, `body_temperature`, `spO2` |
| Column Family **meta** | `battery_level`, `heart_rate_imputed` |

**Row Key:** As the documentation states, never use a timestamp as a prefix in the row key, as this would lead to inefficient scans across the key space for queries that are centered around a specific sensor or patient. With the `sensor_id` as a prefix, Bigtable efficiently distributes the rows across nodes.

**Hotspot Prevention:** By bucketing data into hourly intervals, we ensure that queries for the last hour only need to read at most two rows (current hour and previous hour), which prevents hotspots and ensures low-latency access.

**Cell Families:** Separating vitals and metadata into different column families allows for more efficient storage and retrieval, as queries that only need vitals can ignore the metadata columns, and vice versa.