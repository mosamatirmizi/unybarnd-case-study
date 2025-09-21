{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['asin', 'seller_sku']
) }}

select
  ingested_at,
  asin,
  seller_sku,
  title,
  brand,
  model,
  attributes_json,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_catalog') }}
