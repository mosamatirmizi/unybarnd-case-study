{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['amazon_order_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`order_item_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`amazon_order_id`) REFERENCES {{ ref('raw_amazon_order') }}(`amazon_order_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  order_date,
  order_item_id,
  amazon_order_id,
  asin,
  seller_sku,
  quantity_ordered,
  item_price_amount,
  tax_amount,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_order_item') }}
