{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['amazon_order_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`shipment_item_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`amazon_order_id`) REFERENCES {{ ref('raw_amazon_order') }}(`amazon_order_id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  shipment_item_id,
  shipment_id,
  amazon_order_id,
  ship_date,
  delivery_date,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_amazon', 'raw_amazon_shipment_item') }}
