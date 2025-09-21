{{ config(
    materialized='table',
    partition_by={'field': 'ingested_at', 'data_type': 'timestamp', 'granularity': 'day'},
    require_partition_filter=True,
    cluster_by=['campaign_id']
) }}

select
  ingested_at,
  campaign_id,
  name,
  channel,
  start_ts,
  end_ts,
  load_at,
  load_id,
  source_file,
  source_ts,
  ingestion_uuid
from {{ source('raw_common', 'raw_campaign') }}
