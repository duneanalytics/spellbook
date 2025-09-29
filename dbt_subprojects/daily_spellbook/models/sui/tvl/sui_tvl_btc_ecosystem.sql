{{ config(
    schema='sui_tvl',
    alias='btc_ecosystem',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'object_type', 'market_id'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','btc','ecosystem']
) }}

-- BTC Ecosystem: One row per economic object (pool/market/registry)
-- Each row represents one live object with its complete TVL and metadata
-- Eliminates double counting while capturing all essential data

-- DEX Pools - One row per pool
select 
    block_date
    
    -- Object identification
    , pool_id as market_id
    , concat(coin_a_symbol, '/', coin_b_symbol, ' ', 
           coalesce(cast(fee_rate_percent as varchar), '0'), '%') as token_name
    , 'Liquidity Pool' as object_type
    , 'dex' as storage_location
    , protocol
    
    -- Token composition (both tokens in one row)
    , coin_type_a
    , coin_a_symbol as token_a_symbol
    , coin_type_b
    , coin_b_symbol as token_b_symbol
    
    -- Pool TVL amounts (native + USD for both tokens)
    , avg_coin_a_amount as token_a_native_amount
    , avg_coin_b_amount as token_b_native_amount
    , coin_a_price_usd as token_a_price_usd
    , coin_b_price_usd as token_b_price_usd
    , tvl_usd as total_pool_tvl_usd
    
    -- Protocol Fee Rate (available for DEX)
    , fee_rate_percent as protocol_fee_rate
    
    -- Object metadata
    , total_volume_usd as object_volume_usd
    , num_records as data_points_count
    , pool_name as object_name
    
    -- NULL padding for lending/supply specific columns
    , cast(null as varchar) as collateral_coin_symbol
    , cast(null as decimal(38,8)) as btc_collateral
    , cast(null as decimal(38,8)) as btc_borrow
    , cast(null as decimal(38,8)) as btc_supply
    , cast(null as decimal(38,8)) as btc_collateral_usd
    , cast(null as decimal(38,8)) as btc_borrow_usd
    , cast(null as decimal(38,8)) as btc_supply_usd
    , cast(null as decimal(38,8)) as total_btc_supply
    , cast(null as map(varchar, bigint)) as supply_breakdown_json
    , cast(null as decimal(38,8)) as total_btc_usd_value

from {{ ref('sui_tvl_dex_pools_gold') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}

union all

-- Lending Markets - One row per market
select 
    block_date
    
    -- Object identification  
    , concat(protocol, '_', collateral_coin_symbol) as market_id
    , concat(protocol, ' - ', collateral_coin_symbol, ' Market') as token_name
    , case 
        when protocol = 'bucket' then 'Collateral Vault'
        else 'Lending Market'
    end as object_type
    , 'lending' as storage_location
    , protocol
    
    -- NULL padding for DEX specific columns
    , cast(null as varchar) as coin_type_a
    , cast(null as varchar) as token_a_symbol
    , cast(null as varchar) as coin_type_b
    , cast(null as varchar) as token_b_symbol
    , cast(null as decimal(38,8)) as token_a_native_amount
    , cast(null as decimal(38,8)) as token_b_native_amount
    , cast(null as decimal(38,8)) as token_a_price_usd
    , cast(null as decimal(38,8)) as token_b_price_usd
    
    -- Market TVL (collateral represents the TVL locked in this market)
    , btc_collateral_usd as total_pool_tvl_usd
    
    -- Protocol Fee Rate (NOT available for lending protocols)
    , cast(null as decimal) as protocol_fee_rate
    
    -- NULL padding for DEX specific metadata
    , cast(null as decimal(38,8)) as object_volume_usd
    , cast(null as bigint) as data_points_count
    , concat(protocol, ' ', collateral_coin_symbol, ' Market') as object_name
    
    -- Lending specific columns
    , collateral_coin_symbol
    , btc_collateral
    , btc_borrow
    , btc_supply
    , btc_collateral_usd
    , btc_borrow_usd
    , btc_supply_usd
    
    -- NULL padding for supply specific columns
    , cast(null as decimal(38,8)) as total_btc_supply
    , cast(null as map(varchar, bigint)) as supply_breakdown_json
    , cast(null as decimal(38,8)) as total_btc_usd_value

from {{ ref('sui_tvl_lending_pools_gold') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}

union all

-- Supply Tracking - One row for total BTC supply
select 
    block_date
    
    -- Object identification
    , 'total_btc_supply' as market_id
    , 'Total BTC Supply' as token_name
    , 'Supply Registry' as object_type
    , 'supply' as storage_location
    , 'supply_tracking' as protocol
    
    -- NULL padding for DEX specific columns
    , cast(null as varchar) as coin_type_a
    , cast(null as varchar) as token_a_symbol
    , cast(null as varchar) as coin_type_b
    , cast(null as varchar) as token_b_symbol
    , cast(null as decimal(38,8)) as token_a_native_amount
    , cast(null as decimal(38,8)) as token_b_native_amount
    , cast(null as decimal(38,8)) as token_a_price_usd
    , cast(null as decimal(38,8)) as token_b_price_usd
    
    -- Supply TVL (total BTC value in ecosystem)
    , total_btc_usd_value as total_pool_tvl_usd
    
    -- Protocol Fee Rate (NOT applicable for supply tracking)
    , cast(null as decimal) as protocol_fee_rate
    
    -- NULL padding for DEX specific metadata
    , cast(null as decimal(38,8)) as object_volume_usd
    , cast(null as bigint) as data_points_count
    , 'BTC Supply Registry' as object_name
    
    -- NULL padding for lending specific columns
    , cast(null as varchar) as collateral_coin_symbol
    , cast(null as decimal(38,8)) as btc_collateral
    , cast(null as decimal(38,8)) as btc_borrow
    , cast(null as decimal(38,8)) as btc_supply
    , cast(null as decimal(38,8)) as btc_collateral_usd
    , cast(null as decimal(38,8)) as btc_borrow_usd
    , cast(null as decimal(38,8)) as btc_supply_usd
    
    -- Supply specific columns
    , total_btc_supply
    , supply_breakdown_json
    , total_btc_usd_value

from {{ ref('sui_tvl_supply_gold') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}

order by 
    block_date desc
    , storage_location