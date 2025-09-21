{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`po_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  po_id,
  supplier_id,
  order_date,
  product_id,
  quantity,
  expected_delivery,
  status,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_purchase_orders') }}
