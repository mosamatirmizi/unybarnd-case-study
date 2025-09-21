{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`order_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`product_id`) REFERENCES {{ ref('raw_pos_product') }}(`product_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  order_id,
  supplier_id,
  order_date,
  product_id,
  quantity,
  delivery_date,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_purchase_orders') }}
