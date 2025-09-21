# Hitex Case Study dbt Project

This project organizes the Hitex data platform into layered transformations that run on BigQuery. Each layer builds on the previous one to standardize, conform, and surface business-ready data sets.

## Layers

- **L0 Raw (per source)**: 1:1 landed tables that mirror source schemas. These models only apply load metadata (ingestion timestamps, source identifiers) before persisting to `l0_raw`.
- **L1 Standardized (Staging)**: Source-shaped tables with normalized data types, decoded enumerations, and flattened structures. Saves to `l1_staging`.
- **L2 Harmonized / Conformed Core**: Canonical entities with surrogate keys, cross-source mappings, and Slowly Changing Dimensions. Stored in `l2_harmonized`.
- **L3 Semantic (Stars)**: Dimensional models (facts and conformed dimensions) aligned to a business process. Written to `l3_semantic`.
- **L4 Analytics / Features** *(optional)*: Wide, aggregated outputs such as customer lifetime value, cohorts, or machine-learning feature tables. Persisted in `l4_analytics`.

## Directory Structure

```
models/
  raw/          # L0 source replicas
  staging/      # L1 standardized views
  harmonized/   # L2 conformed core
  semantic/     # L3 dimensional stars
  analytics/    # L4 analytical features (optional)
analysis/       # Exploratory queries
macros/         # Reusable SQL macros
snapshots/      # Point-in-time captures for SCD handling
seeds/          # CSV seed data
tests/          # Data tests or unit tests
docs/           # Markdown documentation for `dbt docs`
```

Populate each layer with models that adhere to the definitions above. Use shared macros for surrogate key generation, status normalization, and conformance logic to keep transformations consistent across sources.

## Getting Started

1. Ensure `profiles/profiles.yml` points to the `stately-sentry-472319-k7` project and that `/keys/gcp-sa.json` contains a valid service account.
2. Install the adapter with `pip install -r requirements.txt` (includes `dbt-bigquery`) and run `dbt deps` for package sync.
3. Execute `dbt debug` to validate credentials, then run layer-specific commands:
   - `dbt run --select raw` to load L0 replicas.
   - `dbt run --select staging+` to build L0–L1.
   - `dbt run --select harmonized+` or `dbt build --select l2_harmonized` to include tests.
   - `dbt run --select semantic analytics` for downstream marts.
4. Generate documentation with `dbt docs generate` and serve via `dbt docs serve` as needed.

Use `dbt build` in CI to compile, test, and document the entire pipeline.

## BigQuery Configuration

- **Service account**: Create a dedicated service account with `BigQuery Data Editor`, `BigQuery Job User`, and `BigQuery Data Viewer` roles (plus access to any referenced datasets). Export the key as JSON and mount it at `/keys/gcp-sa.json`.
- **Datasets**: dbt will build into datasets derived from the target dataset (`analytics`) plus the layer suffixes (`analytics_l0_raw`, `analytics_l1_staging`, etc.). Pre-create these datasets or grant permissions so dbt can create them.
- **Profiles**: Update `profiles/profiles.yml` if you change dataset names, default job location (`US` vs `EU`), or keyfile path. Both `dev` and `prod` targets currently point to the same project and dataset.
- **Secrets**: In production, consider using Google Secret Manager or workload identity federation instead of distributing raw key files.
