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
  title,
  vendor,
  product_type,
  status,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_shopify', 'raw_shopify_product') }}
