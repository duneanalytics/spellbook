{{ config(
    schema = 'thorchain_silver',
    alias = 'swap_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'swaps', 'dex']
) }}

SELECT
    tx as tx_hash,
    chain,
    from_addr,
    to_addr,
    from_asset,
    from_e8 / 1e8 as from_asset_amount,
    from_e8,
    to_asset,
    to_e8 / 1e8 as to_asset_amount,  
    to_e8,
    memo,
    pool,
    to_e8_min / 1e8 as to_e8_min_amount,
    to_e8_min,
    swap_slip_bp,
    liq_fee_e8 / 1e8 as liq_fee_amount,
    liq_fee_e8,
    liq_fee_in_rune_e8 / 1e8 as liq_fee_in_rune_amount,
    liq_fee_in_rune_e8,
    _direction as direction,
    _streaming as streaming,
    _tx_type as tx_type,
    streaming_count,
    streaming_quantity,
    event_id,
    cast(from_unixtime(block_timestamp / 1e18) as timestamp) as block_time,
    date(from_unixtime(block_timestamp / 1e18)) as block_date,
    date_trunc('month', from_unixtime(block_timestamp / 1e18)) as block_month,
    date_trunc('hour', from_unixtime(block_timestamp / 1e18)) as block_hour,
    block_timestamp as raw_block_timestamp,
    
    -- From asset pricing fields
    CASE 
        WHEN from_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        WHEN from_asset LIKE 'BTC.%' OR from_asset LIKE 'BTC/%' OR from_asset LIKE 'BTC~%' THEN cast('BTC.BTC' as varbinary)
        WHEN from_asset LIKE 'ETH.%' OR from_asset LIKE 'ETH/%' OR from_asset LIKE 'ETH~%' THEN 
            CASE 
                WHEN from_asset = 'ETH.ETH' OR from_asset = 'ETH/ETH' OR from_asset = 'ETH~ETH' THEN cast('ETH.ETH' as varbinary)
                ELSE cast(regexp_replace(from_asset, '^ETH[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN from_asset LIKE 'BSC.%' OR from_asset LIKE 'BSC/%' OR from_asset LIKE 'BSC~%' THEN
            CASE 
                WHEN from_asset = 'BSC.BNB' THEN cast('BSC.BNB' as varbinary)
                ELSE cast(regexp_replace(from_asset, '^BSC[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN from_asset LIKE 'BNB.%' OR from_asset LIKE 'BNB/%' OR from_asset LIKE 'BNB~%' THEN
            CASE 
                WHEN from_asset = 'BNB.BNB' OR from_asset = 'BNB/BNB' THEN cast('BNB.BNB' as varbinary)
                ELSE cast(regexp_replace(from_asset, '^BNB[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN from_asset LIKE 'DOGE.%' OR from_asset LIKE 'DOGE/%' OR from_asset LIKE 'DOGE~%' THEN cast('DOGE.DOGE' as varbinary)
        ELSE cast(from_asset as varbinary)
    END as from_contract_address,
    
    -- To asset pricing fields
    CASE 
        WHEN to_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        WHEN to_asset LIKE 'BTC.%' OR to_asset LIKE 'BTC/%' OR to_asset LIKE 'BTC~%' THEN cast('BTC.BTC' as varbinary)
        WHEN to_asset LIKE 'ETH.%' OR to_asset LIKE 'ETH/%' OR to_asset LIKE 'ETH~%' THEN 
            CASE 
                WHEN to_asset = 'ETH.ETH' OR to_asset = 'ETH/ETH' OR to_asset = 'ETH~ETH' THEN cast('ETH.ETH' as varbinary)
                ELSE cast(regexp_replace(to_asset, '^ETH[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN to_asset LIKE 'BSC.%' OR to_asset LIKE 'BSC/%' OR to_asset LIKE 'BSC~%' THEN
            CASE 
                WHEN to_asset = 'BSC.BNB' THEN cast('BSC.BNB' as varbinary)
                ELSE cast(regexp_replace(to_asset, '^BSC[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN to_asset LIKE 'BNB.%' OR to_asset LIKE 'BNB/%' OR to_asset LIKE 'BNB~%' THEN
            CASE 
                WHEN to_asset = 'BNB.BNB' OR to_asset = 'BNB/BNB' THEN cast('BNB.BNB' as varbinary)
                ELSE cast(regexp_replace(to_asset, '^BNB[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN to_asset LIKE 'DOGE.%' OR to_asset LIKE 'DOGE/%' OR to_asset LIKE 'DOGE~%' THEN cast('DOGE.DOGE' as varbinary)
        ELSE cast(to_asset as varbinary)
    END as to_contract_address,
    
    -- Extract pool information for better analysis
    CASE 
        WHEN pool LIKE '%.%' THEN split_part(pool, '.', 1)
        ELSE 'THOR'
    END as pool_chain,
    
    CASE 
        WHEN pool LIKE '%.%' THEN split_part(pool, '.', 2)
        ELSE pool
    END as pool_asset

FROM {{ source('thorchain', 'swap_events') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('cast(from_unixtime(block_timestamp / 1e18) as timestamp)') }}
{% endif %}
