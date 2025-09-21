{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['inventory_item_id', 'location_id']
) }}

select
  ingested_at,
  inventory_item_id,
  location_id,
  available,
  updated_at,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_shopify', 'raw_shopify_inventory_levels') }}
