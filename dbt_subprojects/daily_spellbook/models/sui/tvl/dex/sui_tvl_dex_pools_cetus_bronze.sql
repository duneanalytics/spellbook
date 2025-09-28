{{ config(
    schema='sui_tvl',
    alias='dex_pools_cetus_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id', 'object_id', 'version'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','dex','cetus']
) }}

-- Cetus DEX pools for TVL calculation (Bronze Layer)

{% set cetus_start_date = "2025-04-01" %}

with filtered_pools_cte as (
    select
        cast(type_ as varchar) as type
        , date as block_date
        , date_trunc('month', date) as block_month
        , from_unixtime(timestamp_ms/1000) as block_time
        , version
        , object_id
        , json_extract_scalar(object_json, '$.id.id') as pool_id
        , json_extract_scalar(object_json, '$.coin_a') as coin_a_amount_raw
        , json_extract_scalar(object_json, '$.coin_b') as coin_b_amount_raw
        , json_extract_scalar(object_json, '$.current_sqrt_price') as current_sqrt_price
t also shows up in the pool name as 0 as wel. I do think its just a decimal showing error as these pools would no rmally have like .01% fee        , json_extract_scalar(object_json, '$.liquidity') as liquidity
        , cast(json_extract_scalar(object_json, '$.current_tick_index.bits') as bigint) as tick_index_bits
        , cast(json_extract_scalar(object_json, '$.tick_spacing') as integer) as tick_spacing
        
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '%0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::Pool<%'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ cetus_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
        {% endif %}
)

, pools_with_metadata_cte as (
    -- Join filtered pools with pool details for coin types
    select
        fp.*
        , pd.coin_type_a
        , pd.coin_type_b
    from filtered_pools_cte fp
    join {{ ref('sui_tvl_dex_pools_cetus_pool_detail') }} pd on fp.pool_id = pd.pool_id
)

select 
    -- Basic metadata
    p.type
    , p.block_time
    , p.block_date
    , p.block_month
    , p.version
    , p.object_id
    , p.pool_id
    
    -- Pool token type information (raw only)
    , p.coin_type_a
    , p.coin_type_b
    
    -- Pool token amounts (raw only)
    , p.coin_a_amount_raw
    , p.coin_b_amount_raw
    
    -- Pool pricing and parameters (raw only)
    , p.current_sqrt_price
    , p.tick_index_bits
    , p.fee_rate
    , p.liquidity
    , p.tick_spacing

from pools_with_metadata_cte p 