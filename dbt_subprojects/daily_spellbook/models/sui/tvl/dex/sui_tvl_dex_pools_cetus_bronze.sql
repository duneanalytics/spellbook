{{ config(
    schema='sui_tvl',
    alias='dex_pools_cetus_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id', 'block_date'],
    tags=['sui','tvl','dex','cetus']
) }}

-- Cetus DEX pools for TVL calculation
-- Converted from Snowflake task to dbt incremental model

{% set cetus_start_date = "2025-09-16" %}

with coin_info_cte as (
    select
        coin_type,
        coin_decimals as decimals,
        coin_symbol as symbol,
        coin_name as name
    from {{ ref('dex_sui_coin_info') }}
),

filtered_pools_cte as (
    select
        cast(type_ as varchar) as type,
        date as block_date,
        date_trunc('month', date) as block_month,
        from_unixtime(timestamp_ms/1000) as block_time,
        version,
        object_id,
        json_extract_scalar(object_json, '$.id.id') as pool_id,
        json_extract_scalar(object_json, '$.coin_a') as coin_a_amount_raw,
        json_extract_scalar(object_json, '$.coin_b') as coin_b_amount_raw,
        json_extract_scalar(object_json, '$.current_sqrt_price') as current_sqrt_price,
        cast(json_extract_scalar(object_json, '$.fee_rate') as integer) as fee_rate,
        json_extract_scalar(object_json, '$.liquidity') as liquidity,
        cast(json_extract_scalar(object_json, '$.current_tick_index.bits') as integer) as tick_index_bits,
        cast(json_extract_scalar(object_json, '$.tick_spacing') as integer) as tick_spacing
        
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '%0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::Pool<%'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ cetus_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
),

pools_with_metadata_cte as (
    -- Join filtered pools with pool details (like Snowflake pattern)
    select
        fp.*,
        pd.coin_type_a,
        pd.coin_type_b
    from filtered_pools_cte fp
    join {{ ref('sui_tvl_dex_pools_cetus_pool_detail') }} pd on fp.pool_id = pd.pool_id
)

select 
    -- Basic metadata
    p.type,
    p.block_time,
    p.block_date,
    p.block_month,
    p.version,
    p.object_id,
    p.pool_id,
    
    -- Pool token type information
    p.coin_type_a,
    p.coin_type_b,
    coin_a_info.symbol as coin_a_symbol,
    coin_b_info.symbol as coin_b_symbol,
    
    -- Pool token amounts (raw and human-readable)
    p.coin_a_amount_raw,
    case when coin_a_info.decimals is not null
        then cast(cast(p.coin_a_amount_raw as double) / 
             power(10, coin_a_info.decimals) as decimal(38,18))
        else cast(null as decimal(38,18)) end as coin_a_amount,
    p.coin_b_amount_raw,
    case when coin_b_info.decimals is not null
        then cast(cast(p.coin_b_amount_raw as double) / 
             power(10, coin_b_info.decimals) as decimal(38,18))
        else cast(null as decimal(38,18)) end as coin_b_amount,
    
    -- Pool pricing and parameters
    p.current_sqrt_price,
    p.tick_index_bits,
    p.fee_rate,
    p.fee_rate / 10000.0 as fee_rate_percent,
    concat(
        coalesce(coin_a_info.symbol, 'UNKNOWN'),
        ' / ',
        coalesce(coin_b_info.symbol, 'UNKNOWN'),
        ' ',
        cast(p.fee_rate / 10000.0 as varchar),
        '%'
    ) as pool_name,
    p.liquidity,
    p.tick_spacing

from pools_with_metadata_cte p
-- Add token information for both sides of the pool  
left join coin_info_cte coin_a_info on p.coin_type_a = coin_a_info.coin_type
left join coin_info_cte coin_b_info on p.coin_type_b = coin_b_info.coin_type 