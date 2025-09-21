{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['order_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`order_line_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`order_id`) REFERENCES {{ ref('raw_wholesale_order') }}(`order_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  order_line_id,
  order_id,
  product_id,
  quantity,
  unit_price,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_order_line') }}
