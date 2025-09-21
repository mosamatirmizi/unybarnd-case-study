{{ config(
    materialized='table',
    partition_by={'field': 'snapshot_date', 'data_type': 'date'},
    require_partition_filter=True,
    cluster_by=['seller_sku', 'fulfillment_center_id']
) }}

select
  snapshot_date,
  seller_sku,
  fulfillment_center_id,
  on_hand,
  reserved,
  inbound,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_inventory_daily') }}
