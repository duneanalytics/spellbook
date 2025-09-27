{{ config(
    schema='sui_tvl',
    alias='dex_pools_momentum_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['pool_id', 'object_id', 'version'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','dex','momentum']
) }}

-- Momentum DEX pools for TVL calculation (Bronze Layer)
-- Raw extraction only, metadata enrichment moved to silver layer

{% set momentum_start_date = "2025-09-25" %}

with filtered_pools_cte as (
    select
        cast(type_ as varchar) as type
        , date as block_date
        , date_trunc('month', date) as block_month
        , from_unixtime(timestamp_ms/1000) as block_time
        , version
        , object_id
        , json_extract_scalar(object_json, '$.id.id') as pool_id
        
        -- Extract token types from object JSON (Momentum specific paths)
        , concat('0x', json_extract_scalar(object_json, '$.type_x.name')) as token_a_type
        , concat('0x', json_extract_scalar(object_json, '$.type_y.name')) as token_b_type
        
        -- Reserve amounts (raw only)
        , json_extract_scalar(object_json, '$.reserve_x') as reserve_a_raw
        , json_extract_scalar(object_json, '$.reserve_y') as reserve_b_raw
        
        -- Pool parameters (raw only)
        , json_extract_scalar(object_json, '$.flash_loan_fee_rate') as flash_loan_fee_rate
        , cast(json_extract_scalar(object_json, '$.tick_spacing') as integer) as tick_spacing
        , cast(json_extract_scalar(object_json, '$.swap_fee_rate') as integer) as swap_fee_rate
        , json_extract_scalar(object_json, '$.sqrt_price') as sqrt_price
        , json_extract_scalar(object_json, '$.fee_growth_global_x') as fee_growth_global_a
        , json_extract_scalar(object_json, '$.fee_growth_global_y') as fee_growth_global_b
        
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '0x70285592c97965e811e0c6f98dccc3a9c2b4ad854b3594faab9597ada267b860::pool::Pool<%'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ momentum_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
        {% endif %}
)

select 
    -- Timestamps & Identifiers
    p.block_time
    , p.block_date
    , p.block_month
    , p.object_id
    , p.version
    , p.pool_id
    
    -- Token Info (raw only, no metadata)
    , p.token_a_type
    , p.token_b_type
    
    -- Reserves (raw only)
    , p.reserve_a_raw
    , p.reserve_b_raw
    
    -- Pool Parameters (raw only)
    , p.flash_loan_fee_rate
    , p.tick_spacing
    , p.swap_fee_rate
    , p.sqrt_price
    , p.fee_growth_global_a
    , p.fee_growth_global_b
    
    , p.type

from filtered_pools_cte p 