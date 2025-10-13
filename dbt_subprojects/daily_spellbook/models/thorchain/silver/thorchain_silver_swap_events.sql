{{ config(
    schema = 'thorchain_silver',
    alias = 'swap_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'swaps', 'dex']
) }}

with base as (
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
    cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
    date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
    date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
    date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
    block_timestamp as raw_block_timestamp,
    
    -- From asset pricing fields - simplified approach using direct asset identifiers
    CASE 
        WHEN from_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        -- For prices.usd table, we use the exact asset identifier as it appears
        ELSE cast(from_asset as varbinary)
    END as from_contract_address,
    
    -- To asset pricing fields - simplified approach using direct asset identifiers
    CASE 
        WHEN to_asset LIKE 'THOR.%' THEN cast(null as varbinary)
        -- For prices.usd table, we use the exact asset identifier as it appears
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
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '15' day
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.block_time') }}
{% endif %}
