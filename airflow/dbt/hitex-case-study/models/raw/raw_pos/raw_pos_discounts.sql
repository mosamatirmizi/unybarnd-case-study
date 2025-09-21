{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['discount_id', 'receipt_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`receipt_id`) REFERENCES {{ ref('raw_pos_receipt') }}(`receipt_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  discount_id,
  receipt_id,
  type,
  value,
  applied_date,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_discounts') }}
