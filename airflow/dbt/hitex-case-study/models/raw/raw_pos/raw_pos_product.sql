{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`product_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`category_id`) REFERENCES {{ ref('raw_pos_product_category') }}(`category_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  product_id,
  sku,
  product_name,
  category_id,
  description,
  unit_price,
  weight,
  dimensions,
  created_at,
  updated_at,
  status,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_product') }}
