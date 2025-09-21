{{ config(
    materialized='table',
    partition_by={'field': 'snapshot_date', 'data_type': 'date'},
    require_partition_filter=True,
    cluster_by=['product_id', 'store_id'],
    post_hook=[
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`store_id`) REFERENCES {{ ref('raw_pos_store') }}(`store_id`) NOT ENFORCED",
        "ALTER TABLE {{ this }} ADD FOREIGN KEY (`product_id`) REFERENCES {{ ref('raw_pos_product') }}(`product_id`) NOT ENFORCED"
    ]
) }}

select
  snapshot_date,
  store_id,
  product_id,
  on_hand,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_pos', 'raw_pos_inventory_daily') }}
