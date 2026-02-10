# Ingestion And Cleaning Logic

The ingestion script reads line-delimited JSON from `data/vitals_raw.txt` and applies a minimal, clinically-aware cleaning pass before writing `data/vitals_cleaned.jsonl`.

## Cleaning Steps

- Parse `event_timestamp` with `errors='coerce'` and drop any rows that become `NaT`.
- Drop any rows where `event_timestamp` is in the future relative to UTC.
- Trim any `body_temperature` values out of the plausible human range (27-42.6°C) to the nearest bound.
- For missing `heart_rate`, impute per `sensor_id` using a rolling mean (window=3, `min_periods=1`) after sorting by `event_timestamp`.
- Keep a boolean flag `heart_rate_imputed` to preserve data lineage.
- If a `heart_rate` remains missing after imputation (e.g., all values missing for a sensor), drop the row.

## Drop vs Impute Null values

As heart rate values are often analyzed as continous time series, dropping nulls would break analytics. Therefore, imputation was included using a rolling window per sensor. For data lineage, `heart_rate_imputed` is set to `True` for these values.

# Schema Defense

The detailed Bigtable and BigQuery schema justification lives in [design_docs/SCHEMA_DEFENSE.md](design_docs/SCHEMA_DEFENSE.md).

# BigQuery Analytics Layer

BQ serves as our long-term analytics warehouse.

## Schema

The BigQuery table is a flat, denormalized representation — one row per sensor reading — which is the natural shape for SQL analytics and avoids the need for `UNNEST` operations.

| Column | Type | Notes |
| --- | --- | --- |
| `event_timestamp` | `TIMESTAMP` | Converted from Unix ms; partition key |
| `sensor_id` | `STRING` | Clustering key |
| `heart_rate` | `FLOAT64` | bpm |
| `body_temperature` | `FLOAT64` | °C |
| `spO2` | `INT64` | % |
| `battery_level` | `INT64` | % |
| `heart_rate_imputed` | `BOOL` | Data lineage flag |

## Sustained Alerts Query

The analytical query in `sql/analytical_query.sql` detects sensors where `body_temperature > 40 °C` for **3 or more consecutive readings** using a gaps-and-islands technique. Two `ROW_NUMBER()` sequences are computed per sensor: one over all readings, one over only the hot readings. Their difference stays constant for consecutive hot readings, forming a natural `streak_id`. Grouping by `(sensor_id, streak_id)` and filtering to length >= 3 yields the sustained alerts.

## Optimizing for 1 Petabyte

At petabyte scale this table would span years of data across thousands of ICU monitors. The two most impactful optimisations are **partitioning** and **clustering**.

**Partition Key `event_timestamp` (DAY):** Most analytical queries include a time-range predicate (e.g. "last 30 days"). Day-level partitioning lets BigQuery prune entire days from the scan, eliminating terabytes of I/O before the query engine starts. Day granularity is the right trade-off — hourly would exceed BigQuery's partition limits (4,000 for ingestion-time, 10,000 for column-based) over years of data, while monthly would be too coarse.

**Clustering on `sensor_id`:** Nearly every query filters or groups by sensor. Clustering co-locates data for the same sensor within each partition, so a query for "sensor X in the last 7 days" reads only the relevant blocks instead of scanning entire day partitions.

**Clustering on `body_temperature`:** As a secondary clustering column, BigQuery can skip blocks where the temperature range doesn't overlap threshold predicates like `body_temperature > 40` — effectively a lightweight index for alert-type queries.

**Additional considerations at scale:**
- **Materialized Views** for pre-aggregated alert metrics (e.g. hourly max temperature per sensor) to avoid re-scanning raw data.
- **BI Engine Reservation** to cache frequently-accessed partitions in memory for sub-second dashboard responses.
- **Storage Lifecycle Policies** — data untouched for 90 days automatically drops to ~50% of the active storage cost, significant at petabyte scale.

# Vertex AI Pipeline

The pipeline in `code/vertex_pipeline.py` automates the path from data to deployed model. It uses KFP v2 (`@dsl.component` / `@dsl.pipeline`) compiled to YAML for execution on Vertex AI Pipelines.

**Pipeline steps:** Ingest from BQ (derive `septic_risk` label) ==> Train dummy sklearn `LogisticRegression` ==> Register model in Vertex AI Model Registry ==> Deploy to Vertex AI Endpoint.

**Label derivation:** A binary `septic_risk` flag is computed in the ingestion query: `body_temperature > 38.5 AND heart_rate > 100`. This is a placeholder — a production model would use clinically validated SIRS/qSOFA criteria and richer features.

**Serving container:** The pipeline uses Google's pre-built `sklearn-cpu` serving image, which handles model loading and REST inference out of the box.

## Model Drift Feedback Loop

If model drift detection triggers an alert in production, the following automated steps should execute:

**1. Detect:** Vertex AI Model Monitoring continuously compares incoming prediction request distributions against the training baseline. Feature drift is measured via Jensen-Shannon divergence; prediction drift via distribution shift on model outputs. Thresholds are configured per feature (e.g. `body_temperature` distribution shift > 0.1).

**2. Alert:** When drift exceeds a threshold, Cloud Monitoring fires a notification to a Pub/Sub topic (`model-drift-alerts`). This decouples detection from action and allows multiple subscribers.

**3. Retrain:** A Cloud Function subscribed to the drift topic re-submits the Vertex AI pipeline with a fresh BigQuery window (e.g. last 30 days). The pipeline runs the same Ingest ==> Train ==> Register steps, producing a candidate model.

**4. Validate:** The candidate model's metrics (AUC, precision, recall) are compared against the current production model using a hold-out evaluation set. Only if the candidate outperforms the champion is it promoted — this is a champion/challenger gate that prevents regressions.

**5. Deploy:** The pipeline updates the existing Endpoint's traffic split to route 100% to the new model version (blue-green swap). Alternatively, a canary rollout (e.g. 10% ==> 50% ==> 100%) can be used for higher-risk deployments.

**6. Log:** All retraining events, model versions, evaluation metrics, and drift scores are tracked in Vertex AI Experiments. This provides a full audit trail linking each deployed model back to the drift signal that triggered its retraining.