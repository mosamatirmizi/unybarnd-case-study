{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True
) }}

select
  ingested_at,
  jurisdiction,
  tax_category,
  rate,
  valid_from,
  valid_to,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_common', 'raw_tax_rule') }}
