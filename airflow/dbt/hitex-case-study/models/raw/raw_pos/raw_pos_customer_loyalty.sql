{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`customer_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  customer_id,
  name,
  email,
  contact_info,
  points_balance,
  last_purchase_date,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_customer_loyalty') }}
