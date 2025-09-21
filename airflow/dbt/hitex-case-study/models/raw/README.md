# L0 Raw

This layer mirrors upstream ingestion files one-to-one while enforcing a
baseline set of contractual guardrails:

- **Schema authority** – The canonical contract for every relation lives in
  `SQL/create_raw_tables.sql`. `_raw_sources.yml` is generated from that DDL so
  column order, naming and descriptions stay consistent across Airflow and dbt.
- **Partition discipline** – Every table declares `require_partition_filter =
  true` in both BigQuery DDL and dbt configs. Downstream queries must always
  filter by the declared partition key (`ingested_at` or the relevant snapshot
  date column) to avoid expensive full table scans.
- **Metadata completeness** – Each dataset carries a shared block of
  observability columns (`load_at`, `load_id`, `session_id` when applicable,
  `source_file`, `source_ts`, `ingestion_uuid`). These are surfaced verbatim in
  the raw models and documented via `persist_docs` so the contract is visible in
  the warehouse UI.

### Working with this layer

1. Update `SQL/create_raw_tables.sql` first whenever the contract changes.
2. Regenerate `_raw_sources.yml` from the DDL (for example by rerunning the
   helper notebook or utility referenced in the root README) so the dbt source
   definitions inherit the latest column contract.
3. Ensure any ingestion DAG that writes to the table emits the full metadata
   block and honors the partitioning rules.
4. Raw dbt models should continue to select the source relation wholesale. Any
   transformations belong in the staging layer and above.

Following this workflow keeps ingestion code, documentation, and warehouse
constraints aligned – meeting the non-functional requirements for observability
and predictable performance.
