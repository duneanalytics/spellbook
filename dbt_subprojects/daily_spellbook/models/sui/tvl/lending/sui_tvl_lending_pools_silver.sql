{{ config(
    schema='sui_tvl',
    alias='lending_pools_silver',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'protocol', 'market_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','lending','silver']
) }}

-- Silver layer: Daily lending pools with end-of-day calculations

with 
-- 1. Get new raw data since the last run
raw_updates as (
    select *
    from {{ ref('sui_tvl_lending_pools_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
)

-- 2. For fetching the latest snapshot for each market per day
, raw_updates_with_rn as (
    select *
           , row_number() over (
               partition by block_date, protocol, market_id 
               order by version desc
           ) as rn
    from raw_updates
)

-- 3. Coin Info (including SUI)
, coin_info_cte as (
    select
        coin_type
        , coin_decimals
        , coin_symbol
    from {{ ref('dex_sui_coin_info') }}
)

-- 4. Finding last market state each day
, last_daily_markets as (
    select
        raw.timestamp_ms
        , raw.block_date
        , raw.block_time
        , raw.protocol
        , raw.market_id
        
                -- Collateral Info
        , raw.coin_type as collateral_coin_type
        , ci.coin_symbol as collateral_coin_symbol
        , case 
            when raw.protocol = 'suilend' and raw.coin_type = '0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI' then
                -- Suilend SUI token has extra scaling (÷ 10^12 beyond standard 9 decimals)
                cast(cast(raw.coin_collateral_amount as double) / 
                     (power(10, 9) * power(10, 12)) as decimal(38,8))
            when ci.coin_decimals is not null then
                case when raw.protocol = 'navi' then
                    -- Navi stores values with an extra decimal place
                    cast(cast(raw.coin_collateral_amount as double) / 
                         (power(10, ci.coin_decimals) * 10) as decimal(38,8))
                else
                    cast(cast(raw.coin_collateral_amount as double) / 
                         power(10, ci.coin_decimals) as decimal(38,8))
                end
            else cast(null as decimal(38,8)) 
        end as adjusted_collateral_amount
        
        -- Borrow Info (Handle Bucket Protocol's BUCK token)
        , case 
            when raw.protocol = 'bucket' 
            then buck_ci.coin_type 
            else raw.coin_type 
        end as borrow_coin_type
        , case 
            when raw.protocol = 'bucket' 
            then buck_ci.coin_symbol 
            else ci.coin_symbol 
        end as borrow_coin_symbol
        , case 
            when raw.protocol = 'bucket' and buck_ci.coin_decimals is not null then
                cast(cast(raw.coin_borrow_amount as double) / 
                     power(10, buck_ci.coin_decimals) as decimal(38,8))
            when raw.protocol = 'navi' and ci.coin_decimals is not null then
                -- Navi stores values with an extra decimal place
                cast(cast(raw.coin_borrow_amount as double) / 
                     (power(10, ci.coin_decimals) * 10) as decimal(38,8))
            when raw.protocol = 'suilend' then
                -- Suilend stores borrowed_amount.value in fixed 26-decimal precision
                cast(cast(raw.coin_borrow_amount as double) / 
                     power(10, 26) as decimal(38,8))
            when raw.protocol != 'bucket' and raw.protocol != 'navi' and raw.protocol != 'suilend' and ci.coin_decimals is not null then
                cast(cast(raw.coin_borrow_amount as double) / 
                     power(10, ci.coin_decimals) as decimal(38,8))
            else cast(null as decimal(38,8))
        end as adjusted_borrow_amount
        
    from raw_updates_with_rn raw
    left join coin_info_cte ci on lower(raw.coin_type) = ci.coin_type
    -- Special joins for Bucket Protocol's BUCK borrow asset (case-insensitive)
    left join coin_info_cte buck_ci on buck_ci.coin_type = lower('0x9e3dab13212b27f5434416939db5dec6f6717822e825121b82320b9e8503bade::buck::BUCK')
    where raw.rn = 1 -- Only process the latest snapshot per day
)

-- 5. Final output
select
    block_date
    , protocol
    , market_id
    , collateral_coin_type
    , collateral_coin_symbol
    , adjusted_collateral_amount as eod_collateral_amount
    , borrow_coin_type
    , borrow_coin_symbol
    , adjusted_borrow_amount as eod_borrow_amount
    , timestamp_ms
    , block_time as latest_block_time
from last_daily_markets 