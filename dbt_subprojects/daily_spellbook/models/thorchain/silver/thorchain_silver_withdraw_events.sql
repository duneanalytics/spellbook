{{ config(
    schema = 'thorchain_silver',
    alias = 'withdraw_events',
    materialized = 'view',
    tags = ['thorchain', 'liquidity', 'withdraw_events']  
) }}

with base as (
    SELECT
    tx AS tx_id,
    chain AS blockchain,
    from_addr AS from_address,
    to_addr AS to_address,
    asset,
    asset_e8,
    emit_asset_e8,
    emit_rune_e8,
    memo,
    pool AS pool_name,
    stake_units,
    basis_points,
    asymmetry,
    imp_loss_protection_e8,
    _emit_asset_in_rune_e8,
    event_id,
    block_timestamp,
    _TX_TYPE,
    
    cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
    date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
    date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
    date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
    
    CASE 
        WHEN asset LIKE 'THOR.%' THEN cast(null as varbinary)
        ELSE cast(asset as varbinary)
    END as contract_address,
    
    CASE 
        WHEN pool LIKE '%.%' THEN split_part(pool, '.', 1)
        ELSE 'THOR'
    END as pool_chain,
    
    CASE 
        WHEN pool LIKE '%.%' THEN split_part(pool, '.', 2)
        ELSE pool
    END as pool_asset

    FROM {{ source('thorchain', 'withdraw_events') }}
)

SELECT * FROM base
