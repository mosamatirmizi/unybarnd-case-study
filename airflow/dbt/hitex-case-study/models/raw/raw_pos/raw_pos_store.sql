{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`store_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  store_id,
  store_name,
  address_json,
  latitude,
  longitude,
  timezone,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_store') }}
