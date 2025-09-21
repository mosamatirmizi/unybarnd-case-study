{{ config(
    materialized='table',
    partition_by={'field': 'order_ts', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`order_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`org_account_id`) REFERENCES {{ ref('raw_wholesale_account') }}(`org_account_id`) NOT ENFORCED"
    ]
) }}

select
  order_id,
  order_number,
  org_account_id,
  order_ts,
  status,
  currency,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_order') }}
