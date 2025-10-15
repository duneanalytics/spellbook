{{ config(
    schema = 'thorchain_silver',
    alias = 'swaps',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'swaps', 'curated']
) }}

-- Get block heights for joining to prices
WITH block_heights AS (
    SELECT DISTINCT
        CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) AS block_time,
        height AS block_id
    FROM {{ source('thorchain','block_log') }}
    WHERE CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) >= current_date - interval '16' day
),

swap_events AS (
    SELECT 
        se.tx_hash,
        se.block_time,
        se.block_date,
        se.block_month,
        se.block_hour,
        se.chain,
        se.from_addr,
        se.to_addr,
        se.from_asset,
        se.from_asset_amount,
        se.from_e8,
        se.to_asset,
        se.to_asset_amount,
        se.to_e8,
        se.memo,
        se.pool,
        se.to_e8_min_amount,
        se.swap_slip_bp,
        se.liq_fee_amount,
        se.liq_fee_in_rune_amount,
        se.direction,
        se.streaming,
        se.streaming_count,
        se.streaming_quantity,
        se.from_contract_address,
        se.to_contract_address,
        se.pool_chain,
        se.pool_asset,
        se.event_id,
        bh.block_id,
        'swap_event' as source_table
    FROM {{ ref('thorchain_silver_swap_events') }} se
    LEFT JOIN block_heights bh ON se.block_time = bh.block_time
    WHERE se.block_time >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('se.block_time') }}
    {% endif %}
),

streaming_swaps AS (
    SELECT 
        ss.tx_id as tx_hash,
        ss.block_time,
        ss.block_date,
        ss.block_month,
        ss.block_hour,
        null as chain,
        null as from_addr,
        null as to_addr,
        ss.in_asset as from_asset,
        ss.in_amount as from_asset_amount,
        ss.in_e8 as from_e8,
        ss.out_asset as to_asset,
        ss.out_amount as to_asset_amount,
        ss.out_e8 as to_e8,
        null as memo,
        null as pool,
        null as to_e8_min_amount,
        null as swap_slip_bp,
        null as liq_fee_amount,
        null as liq_fee_in_rune_amount,
        null as direction,
        true as streaming,
        ss.stream_count as streaming_count,
        ss.quantity as streaming_quantity,
        ss.in_contract_address as from_contract_address,
        ss.out_contract_address as to_contract_address,
        null as pool_chain,
        null as pool_asset,
        ss.event_id,
        bh.block_id,
        'streaming_swap_event' as source_table
    FROM {{ ref('thorchain_silver_streaming_swap_details_events') }} ss
    LEFT JOIN block_heights bh ON ss.block_time = bh.block_time
    WHERE ss.block_time >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('ss.block_time') }}
    {% endif %}
)

-- Union all swap types
-- Using Snowflake approach: join on block_id + pool_name
SELECT 
    s.tx_hash,
    s.block_time,
    s.block_date,
    s.block_month,
    s.block_hour,
    s.chain,
    s.from_addr,
    s.to_addr,
    s.from_asset,
    s.from_asset_amount,
    s.from_e8,
    s.to_asset,
    s.to_asset_amount,
    s.to_e8,
    s.memo,
    s.pool,
    s.to_e8_min_amount,
    s.swap_slip_bp,
    s.liq_fee_amount,
    s.liq_fee_in_rune_amount,
    s.direction,
    s.streaming,
    s.streaming_count,
    s.streaming_quantity,
    s.from_contract_address,
    s.to_contract_address,
    s.pool_chain,
    s.pool_asset,
    s.event_id,
    s.source_table,
    
    -- Calculate USD values using pool-based prices (Snowflake approach)
    -- Join on block_id + pool_name for accurate DEX pricing
    s.from_asset_amount * COALESCE(p.asset_usd, p.rune_usd, 0) as from_amount_usd,
    s.to_asset_amount * COALESCE(p.asset_usd, p.rune_usd, 0) as to_amount_usd,
    
    -- Trading metrics
    CASE 
        WHEN COALESCE(p.asset_usd, p.rune_usd, 0) * s.from_asset_amount > 0 
        THEN (COALESCE(p.asset_usd, p.rune_usd, 0) * s.to_asset_amount) / 
             (COALESCE(p.asset_usd, p.rune_usd, 1) * s.from_asset_amount)
        ELSE null 
    END as price_impact,
    
    -- Identify if this is a RUNE trade
    CASE 
        WHEN s.from_asset LIKE 'THOR.RUNE%' OR s.to_asset LIKE 'THOR.RUNE%' THEN true
        ELSE false
    END as involves_rune

FROM swap_events s
-- Snowflake approach: simple join on block_id + pool_name
LEFT JOIN {{ ref('thorchain_silver_prices') }} p
    ON p.block_id = s.block_id
    AND p.pool_name = s.pool

UNION ALL

SELECT 
    s.tx_hash,
    s.block_time,
    s.block_date,
    s.block_month,
    s.block_hour,
    s.chain,
    s.from_addr,
    s.to_addr,
    s.from_asset,
    s.from_asset_amount,
    s.from_e8,
    s.to_asset,
    s.to_asset_amount,
    s.to_e8,
    s.memo,
    s.pool,
    s.to_e8_min_amount,
    s.swap_slip_bp,
    s.liq_fee_amount,
    s.liq_fee_in_rune_amount,
    s.direction,
    s.streaming,
    s.streaming_count,
    s.streaming_quantity,
    s.from_contract_address,
    s.to_contract_address,
    s.pool_chain,
    s.pool_asset,
    s.event_id,
    s.source_table,
    
    -- Calculate USD values using pool-based prices
    s.from_asset_amount * COALESCE(p.asset_usd, p.rune_usd, 0) as from_amount_usd,
    s.to_asset_amount * COALESCE(p.asset_usd, p.rune_usd, 0) as to_amount_usd,
    
    -- Trading metrics
    CASE 
        WHEN COALESCE(p.asset_usd, p.rune_usd, 0) * s.from_asset_amount > 0 
        THEN (COALESCE(p.asset_usd, p.rune_usd, 0) * s.to_asset_amount) / 
             (COALESCE(p.asset_usd, p.rune_usd, 1) * s.from_asset_amount)
        ELSE null 
    END as price_impact,
    
    -- Identify if this is a RUNE trade
    CASE 
        WHEN s.from_asset LIKE 'THOR.RUNE%' OR s.to_asset LIKE 'THOR.RUNE%' THEN true
        ELSE false
    END as involves_rune

FROM streaming_swaps s
-- Snowflake approach: simple join on block_id + pool_name
LEFT JOIN {{ ref('thorchain_silver_prices') }} p
    ON p.block_id = s.block_id
    AND p.pool_name = s.pool
