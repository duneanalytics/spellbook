{{ config(
    schema='sui_tvl',
    alias='dex_pools_momentum_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id', 'block_date'],
    tags=['sui','tvl','dex','momentum']
) }}

-- Momentum DEX pools for TVL calculation
-- Converted from Snowflake task to dbt incremental model

{% set momentum_start_date = "2025-09-23" %}

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
        
        -- Extract token types from object JSON (Momentum specific paths)
        concat('0x', json_extract_scalar(object_json, '$.type_x.name')) as token_a_type,
        concat('0x', json_extract_scalar(object_json, '$.type_y.name')) as token_b_type,
        
        -- Reserve amounts
        json_extract_scalar(object_json, '$.reserve_x') as reserve_a_raw,
        json_extract_scalar(object_json, '$.reserve_y') as reserve_b_raw,
        
        -- Pool parameters  
        json_extract_scalar(object_json, '$.flash_loan_fee_rate') as flash_loan_fee_rate,
        cast(json_extract_scalar(object_json, '$.tick_spacing') as integer) as tick_spacing,
        cast(json_extract_scalar(object_json, '$.swap_fee_rate') as integer) as swap_fee_rate,
        json_extract_scalar(object_json, '$.sqrt_price') as sqrt_price,
        json_extract_scalar(object_json, '$.fee_growth_global_x') as fee_growth_global_a,
        json_extract_scalar(object_json, '$.fee_growth_global_y') as fee_growth_global_b
        
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::pool::Pool<%'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ momentum_start_date }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
)

select 
    -- Timestamps & Identifiers
    p.block_time,
    p.block_date,
    p.block_month,
    p.object_id as digest,
    p.version,
    p.pool_id,
    
    -- Token Info (use Momentum naming convention)
    p.token_a_type,
    p.token_b_type,
    coalesce(token_a_info.symbol, 'UNK') as token_a_symbol,
    coalesce(token_b_info.symbol, 'UNK') as token_b_symbol,
    coalesce(token_a_info.name, 'Unknown') as token_a_name,
    coalesce(token_b_info.name, 'Unknown') as token_b_name,
    token_a_info.decimals as token_a_decimals,
    token_b_info.decimals as token_b_decimals,
    
    -- Reserves (Raw & Adjusted) - Momentum specific naming
    p.reserve_a_raw,
    p.reserve_b_raw,
    case when token_a_info.decimals is not null
        then cast(cast(p.reserve_a_raw as double) / 
             power(10, token_a_info.decimals) as decimal(38,18))
        else cast(null as decimal(38,18)) end as reserve_a_adjusted,
    case when token_b_info.decimals is not null
        then cast(cast(p.reserve_b_raw as double) / 
             power(10, token_b_info.decimals) as decimal(38,18))
        else cast(null as decimal(38,18)) end as reserve_b_adjusted,
    
    -- Pool Parameters
    p.flash_loan_fee_rate,
    p.tick_spacing,
    p.swap_fee_rate,
    p.swap_fee_rate / 10000.0 as swap_fee_rate_percent,
    p.sqrt_price,
    p.fee_growth_global_a,
    p.fee_growth_global_b,
    
    -- Derived Pool Name
    concat(
        coalesce(token_a_info.symbol, 'UNK'),
        ' / ',
        coalesce(token_b_info.symbol, 'UNK'),
        ' ',
        cast(p.swap_fee_rate / 10000.0 as varchar) || '%'
    ) as pool_name,
    
    p.type

from filtered_pools_cte p
-- Join to get coin info for Token A  
left join coin_info_cte token_a_info on p.token_a_type = token_a_info.coin_type
-- Join to get coin info for Token B
left join coin_info_cte token_b_info on p.token_b_type = token_b_info.coin_type 