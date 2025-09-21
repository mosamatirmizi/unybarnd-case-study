{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`contract_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`org_account_id`) REFERENCES {{ ref('raw_wholesale_account') }}(`org_account_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  contract_id,
  org_account_id,
  start_date,
  discount_type,
  discount_value,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_contract') }}
