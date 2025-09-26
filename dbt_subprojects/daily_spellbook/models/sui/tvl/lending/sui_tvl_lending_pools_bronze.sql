{{ config(
    schema='sui_tvl',
    alias='lending_pools_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'market_id', 'block_date'],
    partition_by=['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags=['sui','tvl','lending']
) }}

-- All lending pools for TVL calculation (Bronze Layer)
-- Covers: Suilend, Navi, Scallop, Bucket, Alphalend

{% set sui_project_start_date = var('sui_project_start_date', '2025-09-25') %}

with suilend_raw as (
    -- Suilend raw objects for array processing
    select
        timestamp_ms,
        from_unixtime(timestamp_ms/1000) as block_time,
        date(from_unixtime(timestamp_ms/1000)) as block_date,
        date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'suilend' as protocol,
        object_json,
        object_id,
        version,
        checkpoint
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '0xf95b06141ed4a174f239417323bde3f209b972f5930d8521ea38a52aff3a6ddf::lending_market::LendingMarket<%>'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'suilend'), 0)
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
),

suilend_base as (
    -- Extract reserves[0] 
    select
        timestamp_ms,
        block_time,
        block_date,
        block_month,
        date_hour,
        protocol,
        json_extract_scalar(object_json, '$.reserves[0].id.id') as market_id,
        case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[0].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[0].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[0].coin_type.name'))
        end as coin_type,
        cast(json_extract_scalar(object_json, '$.reserves[0].ctoken_supply') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.reserves[0].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[0].id.id') is not null
    
    union all
    
    -- Extract reserves[1]
    select
        timestamp_ms,
        block_time,
        block_date,
        block_month,
        date_hour,
        protocol,
        json_extract_scalar(object_json, '$.reserves[1].id.id') as market_id,
        case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[1].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[1].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[1].coin_type.name'))
        end as coin_type,
        cast(json_extract_scalar(object_json, '$.reserves[1].ctoken_supply') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.reserves[1].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[1].id.id') is not null
    
    union all
    
    -- Extract reserves[2]
    select
        timestamp_ms,
        block_time,
        block_date,
        block_month,
        date_hour,
        protocol,
        json_extract_scalar(object_json, '$.reserves[2].id.id') as market_id,
        case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[2].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[2].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[2].coin_type.name'))
        end as coin_type,
        cast(json_extract_scalar(object_json, '$.reserves[2].ctoken_supply') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.reserves[2].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[2].id.id') is not null
    
    union all
    
    -- Extract reserves[3]
    select
        timestamp_ms,
        block_time,
        block_date,
        block_month,
        date_hour,
        protocol,
        json_extract_scalar(object_json, '$.reserves[3].id.id') as market_id,
        case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[3].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[3].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[3].coin_type.name'))
        end as coin_type,
        cast(json_extract_scalar(object_json, '$.reserves[3].ctoken_supply') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.reserves[3].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[3].id.id') is not null
    
    union all
    
    -- Extract reserves[4]
    select
        timestamp_ms,
        block_time,
        block_date,
        block_month,
        date_hour,
        protocol,
        json_extract_scalar(object_json, '$.reserves[4].id.id') as market_id,
        case 
            when starts_with(json_extract_scalar(object_json, '$.reserves[4].coin_type.name'), '0x') 
            then json_extract_scalar(object_json, '$.reserves[4].coin_type.name')
            else concat('0x', json_extract_scalar(object_json, '$.reserves[4].coin_type.name'))
        end as coin_type,
        cast(json_extract_scalar(object_json, '$.reserves[4].ctoken_supply') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.reserves[4].borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from suilend_raw
    where json_extract_scalar(object_json, '$.reserves[4].id.id') is not null
),

navi_base as (
    -- Navi has one object per market, making the extraction straightforward.
    select
        timestamp_ms,
        from_unixtime(timestamp_ms/1000) as block_time,
        date(from_unixtime(timestamp_ms/1000)) as block_date,
        date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'navi' as protocol,
        json_extract_scalar(object_json, '$.value.id') as market_id,
        case 
            when starts_with(json_extract_scalar(object_json, '$.value.coin_type'), '0x') 
            then json_extract_scalar(object_json, '$.value.coin_type')
            else concat('0x', json_extract_scalar(object_json, '$.value.coin_type'))
        end as coin_type,
        cast(json_extract_scalar(object_json, '$.value.supply_balance.total_supply') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.value.borrow_balance.total_supply') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '%::storage::ReserveData>%' -- More generic to catch versions
        and json_extract_scalar(object_json, '$.value.coin_type') is not null
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'navi'), 0)
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
),

scallop_base as (
    -- Scallop also has one object per market.
    -- Total collateral is the sum of cash (available) and debt (borrowed).
    select
        timestamp_ms,
        from_unixtime(timestamp_ms/1000) as block_time,
        date(from_unixtime(timestamp_ms/1000)) as block_date,
        date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'scallop' as protocol,
        cast(object_id as varchar) as market_id,
        case 
            when starts_with(json_extract_scalar(object_json, '$.name.name'), '0x') 
            then json_extract_scalar(object_json, '$.name.name')
            else concat('0x', json_extract_scalar(object_json, '$.name.name'))
        end as coin_type,
        (cast(json_extract_scalar(object_json, '$.value.cash') as decimal(38,0)) + 
         cast(json_extract_scalar(object_json, '$.value.debt') as decimal(38,0))) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.value.debt') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '0x2::dynamic_field::Field<0x1::type_name::TypeName, 0xefe8b36d5b2e43728cc323298626b83177803521d195cfb11e15b910e892fddf::reserve::BalanceSheet>%'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'scallop'), 0)
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
),

bucket_base as (
    -- Bucket has isolated markets. Each Bucket object is a market for one collateral type.
    -- The borrowed asset is always BUCK stablecoin.
    -- Collateral coin type is extracted from the object's struct tag.
    select
        timestamp_ms,
        from_unixtime(timestamp_ms/1000) as block_time,
        date(from_unixtime(timestamp_ms/1000)) as block_date,
        date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'bucket' as protocol,
        cast(object_id as varchar) as market_id,
        case 
            when starts_with(regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1), '0x') 
            then regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1)
            else concat('0x', regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1))
        end as coin_type,
        cast(json_extract_scalar(object_json, '$.collateral_vault') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.minted_buck_amount') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) like '0xce7ff77a83ea0cb6fd39bd8748e2ec89a3f41e8efdc3f4eb123e0ca37b184db2::bucket::Bucket<%>'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'bucket'), 0)
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
),

alphalend_base as (
    -- Alphalend has one object per market, similar to Navi and Scallop.
    -- Total collateral is the sum of available liquidity (balance_holding) and borrowed assets.
    select
        timestamp_ms,
        from_unixtime(timestamp_ms/1000) as block_time,
        date(from_unixtime(timestamp_ms/1000)) as block_date,
        date_trunc('month', from_unixtime(timestamp_ms/1000)) as block_month,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'alphalend' as protocol,
        json_extract_scalar(object_json, '$.value.market_id') as market_id,
        concat('0x', json_extract_scalar(object_json, '$.value.coin_type.name')) as coin_type,
        (cast(json_extract_scalar(object_json, '$.value.balance_holding') as decimal(38,0)) + 
         cast(json_extract_scalar(object_json, '$.value.borrowed_amount') as decimal(38,0))) as coin_collateral_amount,
        cast(json_extract_scalar(object_json, '$.value.borrowed_amount') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from {{ source('sui','objects') }}
    where cast(type_ as varchar) = '0x2::dynamic_field::Field<u64, 0xd631cd66138909636fc3f73ed75820d0c5b76332d1644608ed1c85ea2b8219b4::market::Market>'
        and from_unixtime(timestamp_ms/1000) >= timestamp '{{ sui_project_start_date }}'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'alphalend'), 0)
    and {{ incremental_predicate('from_unixtime(timestamp_ms/1000)') }}
    {% endif %}
)

select timestamp_ms, block_time, block_date, block_month, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from suilend_base
union all
select timestamp_ms, block_time, block_date, block_month, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from navi_base
union all
select timestamp_ms, block_time, block_date, block_month, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from scallop_base
union all
select timestamp_ms, block_time, block_date, block_month, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from bucket_base
union all
select timestamp_ms, block_time, block_date, block_month, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from alphalend_base 