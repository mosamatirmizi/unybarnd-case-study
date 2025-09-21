{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['product_id', 'location_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`tx_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  tx_id,
  product_id,
  location_id,
  quantity,
  tx_type,
  tx_date,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_inventory_transactions') }}
