{{ config(
    materialized='table',
    partition_by={'field': 'snapshot_date', 'data_type': 'date'},
    require_partition_filter=True,
    cluster_by=['sku']
) }}

select
  snapshot_date,
  sku,
  quantity,
  age_category,
  storage_fee,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_inventory_aged') }}
