{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['settlement_id', 'amazon_order_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`amazon_order_id`) REFERENCES {{ ref('raw_amazon_order') }}(`amazon_order_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  settlement_id,
  posted_date,
  transaction_type,
  amazon_order_id,
  amount,
  fee_type,
  fee_amount,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_settlement_tx') }}
