{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`amazon_order_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  amazon_order_id,
  purchase_date,
  last_update_date,
  order_status,
  fulfillment_channel,
  sales_channel,
  buyer_name,
  buyer_email,
  shipping_address_json,
  currency,
  order_total,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_order') }}
