{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (`id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`customer_id`) REFERENCES {{ ref('raw_shopify_customer') }}(`id`) NOT ENFORCED"
    ]
) }}

select
  ingested_at,
  id,
  created_at,
  processed_at,
  financial_status,
  fulfillment_status,
  currency,
  total_items,
  total_price,
  customer_id,
  source_name,
  utm_source,
  utm_medium,
  utm_campaign,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_shopify', 'raw_shopify_order') }}
