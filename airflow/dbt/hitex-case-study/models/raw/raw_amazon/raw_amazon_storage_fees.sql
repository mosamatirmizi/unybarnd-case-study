{{ config(
    materialized='table',
    partition_by={'field': 'fee_date', 'data_type': 'date'},
    require_partition_filter=True,
    cluster_by=['sku']
) }}

select
  fee_date,
  sku,
  quantity,
  fee_amount,
  currency,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_storage_fees') }}
