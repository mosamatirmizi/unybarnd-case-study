{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['order_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`shipment_item_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`order_id`) REFERENCES {{ ref('raw_wholesale_order') }}(`order_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  shipment_item_id,
  shipment_id,
  order_id,
  ship_date,
  quantity,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_wholesale', 'raw_wholesale_shipment_item') }}
