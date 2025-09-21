{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['order_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`order_id`) REFERENCES {{ ref('raw_shopify_order') }}(`id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`product_id`) REFERENCES {{ ref('raw_shopify_product') }}(`id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  id,
  order_id,
  product_id,
  variant_id,
  sku,
  quantity,
  price,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_shopify', 'raw_shopify_order_line') }}
