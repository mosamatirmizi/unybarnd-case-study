{{ config(
    materialized='table',
    partition_by={'field': 'tx_ts', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`payment_tx_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`order_id`) REFERENCES {{ ref('raw_wholesale_order') }}(`order_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`invoice_id`) REFERENCES {{ ref('raw_wholesale_invoice') }}(`invoice_id`) NOT ENFORCED"
    ]
) }}

select
  payment_tx_id,
  invoice_id,
  order_id,
  tx_type,
  amount,
  tx_ts,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_payment_tx') }}
