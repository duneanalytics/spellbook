{{ config(
    schema='sui_tvl',
    alias='dex_pools_bluefin_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id', 'block_date'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','dex','bluefin']
) }}

-- Bluefin DEX pools for TVL calculation
-- Converted from Snowflake task to dbt incremental model

{% set bluefin_start_date = "2025-09-24" %}

with coin_info_cte as (
    -- Use existing DEX coin info model
    select
        coin_type,
        coin_decimals as decimals,
        coin_symbol as symbol
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
        
        -- Extract coin types from the object type string and replace short SUI address with full address
        case
            when regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 1) = '0x2::sui::SUI'
            then '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI'
            else regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 1)
        end as coin_type_a,
        
        case
            when regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 2) = '0x2::sui::SUI'
            then '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI'
            else regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 2)
        end as coin_type_b,
        
        json_extract_scalar(object_json, '$.coin_a') as coin_a_amount_raw,
        json_extract_scalar(object_json, '$.coin_b') as coin_b_amount_raw,
        json_extract_scalar(object_json, '$.current_sqrt_price') as current_sqrt_price,
        cast(json_extract_scalar(object_json, '$.fee_rate') as integer) as fee_rate,
        json_extract_scalar(object_json, '$.liquidity') as liquidity
        
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '%0x3492c874c1e3b3e2984e8c41b589e642d4d0a5d6459e5a9cfc2d52fd7c89c267::pool::Pool<%'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ bluefin_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
        {% endif %}
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
    p.liquidity

from filtered_pools_cte p
-- Add token information for both sides of the pool
left join coin_info_cte coin_a_info on p.coin_type_a = coin_a_info.coin_type
left join coin_info_cte coin_b_info on p.coin_type_b = coin_b_info.coin_type 