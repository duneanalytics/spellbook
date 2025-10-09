{{ config(
    schema = 'thorchain_silver',
    alias = 'add_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'liquidity', 'add_events']
) }}

with base as (
    SELECT
        tx as tx_hash,
        chain,
        from_addr,
        to_addr,
        asset,
        asset_e8 / 1e8 as asset_amount,
        asset_e8,
        memo,
        rune_e8 / 1e8 as rune_amount,
        rune_e8,
        pool,
        _tx_type as tx_type,
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
        
        -- Extract pool information for better analysis
        CASE 
            WHEN pool LIKE '%.%' THEN split_part(pool, '.', 1)
            ELSE 'THOR'
        END as pool_chain,
        
        CASE 
            WHEN pool LIKE '%.%' THEN split_part(pool, '.', 2)
            ELSE pool
        END as pool_asset

    FROM {{ source('thorchain', 'add_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '10' day
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.block_time') }}
{% endif %}
