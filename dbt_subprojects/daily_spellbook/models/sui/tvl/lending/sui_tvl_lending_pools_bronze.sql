{{ config(
    schema='sui_tvl',
    alias='lending_pools_bronze',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'object_id', 'version'],
    tags=['sui','tvl','lending']
) }}

-- All lending pools for TVL calculation (Bronze Layer)
-- Converted from Snowflake materialized view to dbt incremental model
-- Covers: Suilend, Navi, Scallop, Bucket, Alphalend

with suilend_base as (
    -- Suilend uses a single monolith object containing an array of reserves.
    -- We use json_extract to create a row for each reserve in the array.
    select
        timestamp_ms,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'suilend' as protocol,
        json_extract_scalar(reserve_json, '$.id.id') as market_id,
        case 
            when left(json_extract_scalar(reserve_json, '$.coin_type.name'), 2) = '0x' 
            then json_extract_scalar(reserve_json, '$.coin_type.name')
            else concat('0x', json_extract_scalar(reserve_json, '$.coin_type.name'))
        end as coin_type,
        cast(json_extract_scalar(reserve_json, '$.ctoken_supply') as decimal(38,0)) as coin_collateral_amount,
        cast(json_extract_scalar(reserve_json, '$.borrowed_amount.value') as decimal(38,0)) as coin_borrow_amount,
        object_id,
        version,
        checkpoint
    from {{ source('sui','objects') }} o
    cross join unnest(
        cast(json_extract(object_json, '$.reserves') as array(json))
    ) as t(reserve_json)
    where cast(type_ as varchar) like '0xf95b06141ed4a174f239417323bde3f209b972f5930d8521ea38a52aff3a6ddf::lending_market::LendingMarket<%>'
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'suilend'), 0)
    {% endif %}
),

navi_base as (
    -- Navi has one object per market, making the extraction straightforward.
    select
        timestamp_ms,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'navi' as protocol,
        json_extract_scalar(object_json, '$.value.id') as market_id,
        case 
            when left(json_extract_scalar(object_json, '$.value.coin_type'), 2) = '0x' 
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
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'navi'), 0)
    {% endif %}
),

scallop_base as (
    -- Scallop also has one object per market.
    -- Total collateral is the sum of cash (available) and debt (borrowed).
    select
        timestamp_ms,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'scallop' as protocol,
        object_id as market_id,
        case 
            when left(json_extract_scalar(object_json, '$.name.name'), 2) = '0x' 
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
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'scallop'), 0)
    {% endif %}
),

bucket_base as (
    -- Bucket has isolated markets. Each Bucket object is a market for one collateral type.
    -- The borrowed asset is always BUCK stablecoin.
    -- Collateral coin type is extracted from the object's struct tag.
    select
        timestamp_ms,
        date_trunc('hour', from_unixtime(timestamp_ms / 1000)) as date_hour,
        'bucket' as protocol,
        object_id as market_id,
        case 
            when left(regexp_extract(cast(type_ as varchar), '<([^>]+)>', 1), 2) = '0x' 
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
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'bucket'), 0)
    {% endif %}
),

alphalend_base as (
    -- Alphalend has one object per market, similar to Navi and Scallop.
    -- Total collateral is the sum of available liquidity (balance_holding) and borrowed assets.
    select
        timestamp_ms,
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
    {% if is_incremental() %}
    and checkpoint > coalesce((select max(checkpoint) from {{ this }} where protocol = 'alphalend'), 0)
    {% endif %}
)

select timestamp_ms, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from suilend_base
union all
select timestamp_ms, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from navi_base
union all
select timestamp_ms, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from scallop_base
union all
select timestamp_ms, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from bucket_base
union all
select timestamp_ms, date_hour, protocol, market_id, coin_type, coin_collateral_amount, coin_borrow_amount, object_id, version, checkpoint from alphalend_base 