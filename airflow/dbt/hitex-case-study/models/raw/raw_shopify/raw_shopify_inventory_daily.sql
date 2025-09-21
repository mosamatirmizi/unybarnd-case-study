{{ config(
    materialized='table',
    partition_by={'field': 'snapshot_date', 'data_type': 'date'},
    require_partition_filter=True,
    cluster_by=['inventory_item_id', 'location_id']
) }}

select
  snapshot_date,
  inventory_item_id,
  location_id,
  available,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_shopify', 'raw_shopify_inventory_daily') }}
