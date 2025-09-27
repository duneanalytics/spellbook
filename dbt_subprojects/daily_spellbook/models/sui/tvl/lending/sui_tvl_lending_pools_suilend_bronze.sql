{{ config(
    schema='sui_tvl',
    alias='lending_pools_suilend_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'market_id', 'object_id', 'checkpoint'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','lending','suilend']
) }}

-- Suilend lending pools for TVL calculation (Bronze Layer)

{% set sui_project_start_date = var('sui_project_start_date', '2025-09-25') %}

with suilend_raw as (
    -- Suilend raw objects for array processing
    select
        timestamp_ms
        , from_unixtime(timestamp_ms/1000) as block_time
        , date(from_unixtime(timestamp_ms/1000)) as block_date
        , date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month
        , date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour
        , 'suilend' as protocol
        , object_json
        , object_id
        , version
        , checkpoint
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '0xf95b06141ed4a174f239417323bde3f209b972f5930d8521ea38a52aff3a6ddf::lending_market::LendingMarket<%>'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }}), 0)
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
)

, suilend_base as (
    -- Extract reserves[0] 
    select
        timestamp_ms
        , block_time
        , block_date
        , block_month
        , date_hour
        , protocol
        , json_extract_scalar(object_json, '$.reserves[0].id.id') as market_id
        , case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[0].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[0].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[0].coin_type.name'))
        end as coin_type
        , cast(json_extract_scalar(object_json, '$.reserves[0].ctoken_supply') as decimal(38,0)) as coin_collateral_amount
        , cast(json_extract_scalar(object_json, '$.reserves[0].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount
        , object_id
        , version
        , checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[0].id.id') is not null
    
    union all
    
    -- Extract reserves[1]
    select
        timestamp_ms
        , block_time
        , block_date
        , block_month
        , date_hour
        , protocol
        , json_extract_scalar(object_json, '$.reserves[1].id.id') as market_id
        , case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[1].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[1].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[1].coin_type.name'))
        end as coin_type
        , cast(json_extract_scalar(object_json, '$.reserves[1].ctoken_supply') as decimal(38,0)) as coin_collateral_amount
        , cast(json_extract_scalar(object_json, '$.reserves[1].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount
        , object_id
        , version
        , checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[1].id.id') is not null
    
    union all
    
    -- Extract reserves[2]
    select
        timestamp_ms
        , block_time
        , block_date
        , block_month
        , date_hour
        , protocol
        , json_extract_scalar(object_json, '$.reserves[2].id.id') as market_id
        , case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[2].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[2].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[2].coin_type.name'))
        end as coin_type
        , cast(json_extract_scalar(object_json, '$.reserves[2].ctoken_supply') as decimal(38,0)) as coin_collateral_amount
        , cast(json_extract_scalar(object_json, '$.reserves[2].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount
        , object_id
        , version
        , checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[2].id.id') is not null
    
    union all
    
    -- Extract reserves[3]
    select
        timestamp_ms
        , block_time
        , block_date
        , block_month
        , date_hour
        , protocol
        , json_extract_scalar(object_json, '$.reserves[3].id.id') as market_id
        , case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[3].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[3].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[3].coin_type.name'))
        end as coin_type
        , cast(json_extract_scalar(object_json, '$.reserves[3].ctoken_supply') as decimal(38,0)) as coin_collateral_amount
        , cast(json_extract_scalar(object_json, '$.reserves[3].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount
        , object_id
        , version
        , checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[3].id.id') is not null
    
    union all
    
    -- Extract reserves[4]
    select
        timestamp_ms
        , block_time
        , block_date
        , block_month
        , date_hour
        , protocol
        , json_extract_scalar(object_json, '$.reserves[4].id.id') as market_id
        , case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[4].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[4].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[4].coin_type.name'))
        end as coin_type
        , cast(json_extract_scalar(object_json, '$.reserves[4].ctoken_supply') as decimal(38,0)) as coin_collateral_amount
        , cast(json_extract_scalar(object_json, '$.reserves[4].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount
        , object_id
        , version
        , checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[4].id.id') is not null
)

select 
    timestamp_ms
    , block_time
    , block_date
    , block_month
    , date_hour
    , protocol
    , market_id
    , coin_type
    , coin_collateral_amount
    , coin_borrow_amount
    , object_id
    , version
    , checkpoint 
from suilend_base 