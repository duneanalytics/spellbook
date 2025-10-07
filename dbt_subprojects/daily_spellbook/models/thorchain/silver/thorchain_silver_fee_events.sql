{{ config(
    schema = 'thorchain_silver',
    alias = 'fee_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'fees']
) }}

SELECT
    tx as tx_hash,
    asset,
    asset_e8 / 1e8 as asset_amount,
    asset_e8,
    pool_deduct / 1e8 as pool_deduct_amount,
    pool_deduct,
    event_id,
    cast(from_unixtime(block_timestamp / 1e18) as timestamp) as block_time,
    date(from_unixtime(block_timestamp / 1e18)) as block_date,
    date_trunc('month', from_unixtime(block_timestamp / 1e18)) as block_month,
    date_trunc('hour', from_unixtime(block_timestamp / 1e18)) as block_hour,
    block_timestamp as raw_block_timestamp,
    
    -- Asset pricing fields
    CASE 
        WHEN asset LIKE 'THOR.%' THEN cast(null as varbinary)
        WHEN asset LIKE 'BTC.%' OR asset LIKE 'BTC/%' OR asset LIKE 'BTC~%' THEN cast('BTC.BTC' as varbinary)
        WHEN asset LIKE 'ETH.%' OR asset LIKE 'ETH/%' OR asset LIKE 'ETH~%' THEN 
            CASE 
                WHEN asset = 'ETH.ETH' OR asset = 'ETH/ETH' OR asset = 'ETH~ETH' THEN cast('ETH.ETH' as varbinary)
                ELSE cast(regexp_replace(asset, '^ETH[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN asset LIKE 'BSC.%' OR asset LIKE 'BSC/%' OR asset LIKE 'BSC~%' THEN
            CASE 
                WHEN asset = 'BSC.BNB' THEN cast('BSC.BNB' as varbinary)
                ELSE cast(regexp_replace(asset, '^BSC[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN asset LIKE 'BNB.%' OR asset LIKE 'BNB/%' OR asset LIKE 'BNB~%' THEN
            CASE 
                WHEN asset = 'BNB.BNB' OR asset = 'BNB/BNB' THEN cast('BNB.BNB' as varbinary)
                ELSE cast(regexp_replace(asset, '^BNB[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN asset LIKE 'DOGE.%' OR asset LIKE 'DOGE/%' OR asset LIKE 'DOGE~%' THEN cast('DOGE.DOGE' as varbinary)
        ELSE cast(asset as varbinary)
    END as contract_address,
    
    -- Extract asset information for better analysis
    CASE 
        WHEN asset LIKE '%.%' THEN split_part(asset, '.', 1)
        ELSE 'THOR'
    END as asset_chain,
    
    CASE 
        WHEN asset LIKE '%.%' THEN split_part(asset, '.', 2)
        ELSE asset
    END as asset_symbol

FROM {{ source('thorchain', 'fee_events') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('cast(from_unixtime(block_timestamp / 1e18) as timestamp)') }}
{% endif %}
