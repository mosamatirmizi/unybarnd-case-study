{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['supplier_id']
) }}

select
  ingested_at,
  supplier_id,
  supplier_name,
  country,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_common', 'raw_supplier') }}
