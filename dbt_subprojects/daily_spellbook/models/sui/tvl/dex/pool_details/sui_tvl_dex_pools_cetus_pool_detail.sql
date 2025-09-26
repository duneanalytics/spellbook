{{ config(
    schema='sui_tvl',
    alias='dex_pools_cetus_pool_detail',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id'],
    tags=['sui','tvl','dex','cetus','pool_details']
) }}

-- Cetus pool creation events to map pool_id to coin types
-- Converted from Snowflake materialized view to dbt incremental model

{% set cetus_start_date = "2025-01-01" %}

select 
    json_extract_scalar(event_json, '$.pool_id') as pool_id,
    concat('0x', json_extract_scalar(event_json, '$.coin_type_a')) as coin_type_a,
    concat('0x', json_extract_scalar(event_json, '$.coin_type_b')) as coin_type_b,
    json_extract_scalar(event_json, '$.tick_spacing') as tick_spacing,
    transaction_digest,
    event_index,
    epoch,
    checkpoint,
    sender
from {{ source('sui','events') }}
where event_type = '0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::factory::CreatePoolEvent'
    and from_unixtime(timestamp_ms/1000) >= timestamp '{{ cetus_start_date }}'
{% if is_incremental() %}
and checkpoint > coalesce((select max(checkpoint) from {{ this }}), 0)
{% endif %} 