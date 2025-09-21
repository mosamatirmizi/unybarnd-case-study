{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`return_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`receipt_id`) REFERENCES {{ ref('raw_pos_receipt') }}(`receipt_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`product_id`) REFERENCES {{ ref('raw_pos_product') }}(`product_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  return_id,
  receipt_id,
  product_id,
  quantity,
  return_date,
  reason,
  amount,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_returns') }}
