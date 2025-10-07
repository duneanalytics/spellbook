{{ config(
    schema = 'thorchain_silver',
    alias = 'streaming_swap_details_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'swaps', 'streaming']
) }}

SELECT
    tx_id,
    interval as streaming_interval,
    quantity,
    count as stream_count,
    last_height,
    deposit_asset,
    deposit_e8 / 1e8 as deposit_amount,
    deposit_e8,
    in_asset,
    in_e8 / 1e8 as in_amount,
    in_e8,
    out_asset,
    out_e8 / 1e8 as out_amount,
    out_e8,
    failed_swaps,
    failed_swap_reasons,
    event_id,
    cast(from_unixtime(block_timestamp / 1e18) as timestamp) as block_time,
    date(from_unixtime(block_timestamp / 1e18)) as block_date,
    date_trunc('month', from_unixtime(block_timestamp / 1e18)) as block_month,
    date_trunc('hour', from_unixtime(block_timestamp / 1e18)) as block_hour,
    block_timestamp as raw_block_timestamp,
    
    -- Deposit asset pricing fields
    CASE 
        WHEN deposit_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        WHEN deposit_asset LIKE 'BTC.%' OR deposit_asset LIKE 'BTC/%' OR deposit_asset LIKE 'BTC~%' THEN cast('BTC.BTC' as varbinary)
        WHEN deposit_asset LIKE 'ETH.%' OR deposit_asset LIKE 'ETH/%' OR deposit_asset LIKE 'ETH~%' THEN 
            CASE 
                WHEN deposit_asset = 'ETH.ETH' OR deposit_asset = 'ETH/ETH' OR deposit_asset = 'ETH~ETH' THEN cast('ETH.ETH' as varbinary)
                ELSE cast(regexp_replace(deposit_asset, '^ETH[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN deposit_asset LIKE 'BSC.%' OR deposit_asset LIKE 'BSC/%' OR deposit_asset LIKE 'BSC~%' THEN
            CASE 
                WHEN deposit_asset = 'BSC.BNB' THEN cast('BSC.BNB' as varbinary)
                ELSE cast(regexp_replace(deposit_asset, '^BSC[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN deposit_asset LIKE 'BNB.%' OR deposit_asset LIKE 'BNB/%' OR deposit_asset LIKE 'BNB~%' THEN
            CASE 
                WHEN deposit_asset = 'BNB.BNB' OR deposit_asset = 'BNB/BNB' THEN cast('BNB.BNB' as varbinary)
                ELSE cast(regexp_replace(deposit_asset, '^BNB[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN deposit_asset LIKE 'DOGE.%' OR deposit_asset LIKE 'DOGE/%' OR deposit_asset LIKE 'DOGE~%' THEN cast('DOGE.DOGE' as varbinary)
        ELSE cast(deposit_asset as varbinary)
    END as deposit_contract_address,
    
    -- In asset pricing fields
    CASE 
        WHEN in_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        WHEN in_asset LIKE 'BTC.%' OR in_asset LIKE 'BTC/%' OR in_asset LIKE 'BTC~%' THEN cast('BTC.BTC' as varbinary)
        WHEN in_asset LIKE 'ETH.%' OR in_asset LIKE 'ETH/%' OR in_asset LIKE 'ETH~%' THEN 
            CASE 
                WHEN in_asset = 'ETH.ETH' OR in_asset = 'ETH/ETH' OR in_asset = 'ETH~ETH' THEN cast('ETH.ETH' as varbinary)
                ELSE cast(regexp_replace(in_asset, '^ETH[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN in_asset LIKE 'BSC.%' OR in_asset LIKE 'BSC/%' OR in_asset LIKE 'BSC~%' THEN
            CASE 
                WHEN in_asset = 'BSC.BNB' THEN cast('BSC.BNB' as varbinary)
                ELSE cast(regexp_replace(in_asset, '^BSC[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN in_asset LIKE 'BNB.%' OR in_asset LIKE 'BNB/%' OR in_asset LIKE 'BNB~%' THEN
            CASE 
                WHEN in_asset = 'BNB.BNB' OR in_asset = 'BNB/BNB' THEN cast('BNB.BNB' as varbinary)
                ELSE cast(regexp_replace(in_asset, '^BNB[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN in_asset LIKE 'DOGE.%' OR in_asset LIKE 'DOGE/%' OR in_asset LIKE 'DOGE~%' THEN cast('DOGE.DOGE' as varbinary)
        ELSE cast(in_asset as varbinary)
    END as in_contract_address,
    
    -- Out asset pricing fields
    CASE 
        WHEN out_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        WHEN out_asset LIKE 'BTC.%' OR out_asset LIKE 'BTC/%' OR out_asset LIKE 'BTC~%' THEN cast('BTC.BTC' as varbinary)
        WHEN out_asset LIKE 'ETH.%' OR out_asset LIKE 'ETH/%' OR out_asset LIKE 'ETH~%' THEN 
            CASE 
                WHEN out_asset = 'ETH.ETH' OR out_asset = 'ETH/ETH' OR out_asset = 'ETH~ETH' THEN cast('ETH.ETH' as varbinary)
                ELSE cast(regexp_replace(out_asset, '^ETH[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN out_asset LIKE 'BSC.%' OR out_asset LIKE 'BSC/%' OR out_asset LIKE 'BSC~%' THEN
            CASE 
                WHEN out_asset = 'BSC.BNB' THEN cast('BSC.BNB' as varbinary)
                ELSE cast(regexp_replace(out_asset, '^BSC[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN out_asset LIKE 'BNB.%' OR out_asset LIKE 'BNB/%' OR out_asset LIKE 'BNB~%' THEN
            CASE 
                WHEN out_asset = 'BNB.BNB' OR out_asset = 'BNB/BNB' THEN cast('BNB.BNB' as varbinary)
                ELSE cast(regexp_replace(out_asset, '^BNB[./~]([^-]*)-?(.*)$', '$1-$2') as varbinary)
            END
        WHEN out_asset LIKE 'DOGE.%' OR out_asset LIKE 'DOGE/%' OR out_asset LIKE 'DOGE~%' THEN cast('DOGE.DOGE' as varbinary)
        ELSE cast(out_asset as varbinary)
    END as out_contract_address

FROM {{ source('thorchain', 'streaming_swap_details_events') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('cast(from_unixtime(block_timestamp / 1e18) as timestamp)') }}
{% endif %}
