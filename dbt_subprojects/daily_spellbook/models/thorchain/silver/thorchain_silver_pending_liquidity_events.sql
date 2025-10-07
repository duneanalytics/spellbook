{{ config(
    schema = 'thorchain_silver',
    alias = 'pending_liquidity_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'liquidity', 'pending']
) }}

SELECT
    pool,
    asset_tx,
    asset_chain,
    asset_addr,
    asset_e8 / 1e8 as asset_amount,
    asset_e8,
    rune_tx,
    rune_addr,
    rune_e8 / 1e8 as rune_amount,
    rune_e8,
    pending_type,
    event_id,
    cast(from_unixtime(block_timestamp / 1e18) as timestamp) as block_time,
    date(from_unixtime(block_timestamp / 1e18)) as block_date,
    date_trunc('month', from_unixtime(block_timestamp / 1e18)) as block_month,
    date_trunc('hour', from_unixtime(block_timestamp / 1e18)) as block_hour,
    block_timestamp as raw_block_timestamp,
    
    -- Extract pool information for better analysis
    CASE 
        WHEN pool LIKE '%.%' THEN split_part(pool, '.', 1)
        ELSE 'THOR'
    END as pool_chain,
    
    CASE 
        WHEN pool LIKE '%.%' THEN split_part(pool, '.', 2)
        ELSE pool
    END as pool_asset,
    
    -- Asset pricing fields based on pool
    CASE 
        WHEN pool LIKE 'THOR.%' THEN cast(null as varbinary)
        WHEN pool LIKE 'BTC.%' OR pool LIKE 'BTC/%' OR pool LIKE 'BTC~%' THEN cast('BTC.BTC' as varbinary)
        WHEN pool LIKE 'ETH.%' OR pool LIKE 'ETH/%' OR pool LIKE 'ETH~%' THEN 
            CASE 
                WHEN pool = 'ETH.ETH' OR pool = 'ETH/ETH' OR pool = 'ETH~ETH' THEN cast('ETH.ETH' as varbinary)
                ELSE cast(regexp_replace(pool, '^ETH[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN pool LIKE 'BSC.%' OR pool LIKE 'BSC/%' OR pool LIKE 'BSC~%' THEN
            CASE 
                WHEN pool = 'BSC.BNB' THEN cast('BSC.BNB' as varbinary)
                ELSE cast(regexp_replace(pool, '^BSC[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN pool LIKE 'BNB.%' OR pool LIKE 'BNB/%' OR pool LIKE 'BNB~%' THEN
            CASE 
                WHEN pool = 'BNB.BNB' OR pool = 'BNB/BNB' THEN cast('BNB.BNB' as varbinary)
                ELSE cast(regexp_replace(pool, '^BNB[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN pool LIKE 'DOGE.%' OR pool LIKE 'DOGE/%' OR pool LIKE 'DOGE~%' THEN cast('DOGE.DOGE' as varbinary)
        ELSE cast(pool as varbinary)
    END as contract_address

FROM {{ source('thorchain', 'pending_liquidity_events') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('cast(from_unixtime(block_timestamp / 1e18) as timestamp)') }}
{% endif %}
