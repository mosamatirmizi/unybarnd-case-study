{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  id,
  code,
  value,
  usage_limit,
  starts_at,
  ends_at,
  applies_to,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_shopify', 'raw_shopify_discount_codes') }}
