# Schema Defense

This document justifies the hot-storage (Bigtable) and analytics (BigQuery) schema choices, aligned to the query patterns in the challenge.

## Bigtable (Hot Storage)

**Primary query:** "Get all vitals for Patient X for the last 1 hour."

**Row key:** `{sensor_id}#{YYYY-MM-DDTHH}` (UTC hour bucket)

By prefixing with `sensor_id`, reads for a single patient are localized. The hourly bucket keeps the query bounded to at most two rows (current and previous hour), which reduces read amplification. Multiple events within the same hour are stored as cell versions using the event timestamp.

| Property | Value |
| --- | --- |
| Table | `patient_vitals` |
| Row Key | `{sensor_id}#{YYYY-MM-DDTHH}` |
| Column Family `vitals` | `heart_rate`, `body_temperature`, `spO2` |
| Column Family `meta` | `battery_level`, `heart_rate_imputed` |

**Row key rationale:** A timestamp prefix would scatter a single patient's data across the keyspace and make the "last hour" query expensive. Keeping `sensor_id` first allows efficient single-key lookups. Hourly bucketing keeps the number of rows touched by the query minimal.

**Hotspot mitigation:** The write distribution is driven by the `sensor_id` prefix. If very high write concurrency becomes a bottleneck, a hashed prefix (for example, `<hash(sensor_id)%N>#<sensor_id>#<bucket>`) can be introduced to evenly distribute writes without changing the access pattern.

**Column families:** Separating vitals from metadata allows selective reads; dashboards can ignore metadata when only vitals are needed.

## BigQuery (Analytics)

**Table shape:** One row per sensor reading (flat schema). This aligns with SQL analytics and avoids nested data structures.

**Partitioning:** `event_timestamp` by DAY. This prunes scans for time-bounded queries and scales to multi-year data without exceeding partition limits.

**Clustering:** `sensor_id` as the primary clustering key. Most analytics filter or group by sensor, so clustering co-locates those rows within partitions and reduces scan costs.

**At scale (1 PB):**
- Keep DAY partitioning on `event_timestamp` for time-range pruning.
- Retain `sensor_id` clustering to support per-patient slicing.
- Consider a secondary clustering key such as `body_temperature` to accelerate threshold-based alert queries.
- Use materialized views for pre-aggregated alert metrics when dashboards require low latency.
