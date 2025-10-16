{{ config(
    schema = 'thorchain_silver',
    alias = 'pending_liquidity_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'liquidity', 'pending']
) }}

with base as (
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
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
        date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
        block_timestamp as raw_block_timestamp,
        
        CASE 
            WHEN pool LIKE '%.%' THEN split_part(pool, '.', 1)
            ELSE 'THOR'
        END as pool_chain,
        
        CASE 
            WHEN pool LIKE '%.%' THEN split_part(pool, '.', 2)
            ELSE pool
        END as pool_asset,
        
        CASE 
            WHEN pool LIKE 'THOR.%' THEN cast(null as varbinary)
            ELSE cast(pool as varbinary)
        END as contract_address

    FROM {{ source('thorchain', 'pending_liquidity_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
)

SELECT * FROM base
