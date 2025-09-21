{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`warehouse_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  warehouse_id,
  name,
  address,
  capacity,
  current_utilization,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_warehouse') }}
