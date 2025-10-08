{{ config(
    schema = 'thorchain_silver',
    alias = 'fee_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
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
    cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
    date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
    date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
    date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
    block_timestamp as raw_block_timestamp,
    
    -- Asset pricing fields - simplified approach using direct asset identifiers
    CASE 
        WHEN asset LIKE 'THOR.%' THEN cast(null as varbinary)
        -- For prices.usd table, we need to use the exact asset identifier as it appears
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
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
