{{ config(
    materialized='table',
    partition_by={'field': 'return_date', 'data_type': 'date'},
    require_partition_filter=True,
    cluster_by=['amazon_order_id', 'sku'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`amazon_order_id`) REFERENCES {{ ref('raw_amazon_order') }}(`amazon_order_id`) NOT ENFORCED"
    ]
) }}

select
  return_date,
  amazon_order_id,
  sku,
  quantity,
  reason,
  status,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_returns') }}
