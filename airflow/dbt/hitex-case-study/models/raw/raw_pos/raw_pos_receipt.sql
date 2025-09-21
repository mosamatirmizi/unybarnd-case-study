{{ config(
    materialized='table',
    partition_by={'field': 'business_date', 'data_type': 'date'},
    require_partition_filter=True,
    cluster_by=['store_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`receipt_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`store_id`) REFERENCES {{ ref('raw_pos_store') }}(`store_id`) NOT ENFORCED"
    ]
) }}

select
  receipt_id,
  business_date,
  store_id,
  transaction_ts,
  subtotal,
  tax_amount,
  total_amount,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_receipt') }}
