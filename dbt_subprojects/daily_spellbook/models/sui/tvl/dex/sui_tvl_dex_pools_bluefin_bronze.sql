{{ config(
    schema='sui_tvl',
    alias='dex_pools_bluefin_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id', 'object_id', 'version'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','dex','bluefin']
) }}

-- Bluefin DEX pools for TVL calculation (Bronze Layer)
-- Raw extraction only, metadata enrichment moved to silver layer

{% set bluefin_start_date = "2023-08-01" %}

with filtered_pools_cte as (
    select
        cast(type_ as varchar) as type
        , date as block_date
        , date_trunc('month', date) as block_month
        , from_unixtime(timestamp_ms/1000) as block_time
        , version
        , object_id
        , json_extract_scalar(object_json, '$.id.id') as pool_id
        
        -- Extract coin types from the object type string and replace short SUI address with full address
        , case
            when regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 1) = '0x2::sui::SUI'
            then '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI'
            else regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 1)
        end as coin_type_a
        
        , case
            when regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 2) = '0x2::sui::SUI'
            then '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI'
            else regexp_extract(cast(type_ as varchar), '<(.+?), (.+?)>', 2)
        end as coin_type_b
        
        , json_extract_scalar(object_json, '$.coin_a') as coin_a_amount_raw
        , json_extract_scalar(object_json, '$.coin_b') as coin_b_amount_raw
        , json_extract_scalar(object_json, '$.current_sqrt_price') as current_sqrt_price
        , cast(json_extract_scalar(object_json, '$.fee_rate') as integer) as fee_rate
        , json_extract_scalar(object_json, '$.liquidity') as liquidity
        
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '%0x3492c874c1e3b3e2984e8c41b589e642d4d0a5d6459e5a9cfc2d52fd7c89c267::pool::Pool<%'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ bluefin_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
        {% endif %}
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
    , p.fee_rate
    , p.liquidity

from filtered_pools_cte p 