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

-- Curated swaps rollup combining swap events with streaming swap details
WITH swap_events AS (
    SELECT 
        tx_hash,
        block_time,
        block_date,
        block_month,
        block_hour,
        chain,
        from_addr,
        to_addr,
        from_asset,
        from_asset_amount,
        from_e8,
        to_asset,
        to_asset_amount,
        to_e8,
        memo,
        pool,
        to_e8_min_amount,
        swap_slip_bp,
        liq_fee_amount,
        liq_fee_in_rune_amount,
        direction,
        streaming,
        streaming_count,
        streaming_quantity,
        from_contract_address,
        to_contract_address,
        pool_chain,
        pool_asset,
        event_id,
        'swap_event' as source_table
    FROM {{ ref('thorchain_silver_swap_events') }} se
    WHERE se.block_time >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('se.block_time') }}
    {% endif %}
),

streaming_swaps AS (
    SELECT 
        tx_id as tx_hash,
        block_time,
        block_date,
        block_month,
        block_hour,
        null as chain,
        null as from_addr,
        null as to_addr,
        in_asset as from_asset,
        in_amount as from_asset_amount,
        in_e8 as from_e8,
        out_asset as to_asset,
        out_amount as to_asset_amount,
        out_e8 as to_e8,
        null as memo,
        null as pool,
        null as to_e8_min_amount,
        null as swap_slip_bp,
        null as liq_fee_amount,
        null as liq_fee_in_rune_amount,
        null as direction,
        true as streaming,
        stream_count as streaming_count,
        quantity as streaming_quantity,
        in_contract_address as from_contract_address,
        out_contract_address as to_contract_address,
        null as pool_chain,
        null as pool_asset,
        event_id,
        'streaming_swap_event' as source_table
    FROM {{ ref('thorchain_silver_streaming_swap_details_events') }} ss
    WHERE ss.block_time >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('ss.block_time') }}
    {% endif %}
)

-- Union all swap types
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
    
    -- Calculate USD values by joining with prices
    (s.from_asset_amount / POWER(10,8)) * COALESCE(fp_asset.price, fp_rune.price, 0) as from_amount_usd,
    (s.to_asset_amount / POWER(10,8)) * COALESCE(tp_asset.price, tp_rune.price, 0) as to_amount_usd,
    
    -- Trading metrics
    CASE 
        WHEN COALESCE(fp_asset.price, fp_rune.price, 0) * s.from_asset_amount > 0 
        THEN (COALESCE(tp_asset.price, tp_rune.price, 0) * s.to_asset_amount) / (COALESCE(fp_asset.price, fp_rune.price, 1) * s.from_asset_amount)
        ELSE null 
    END as price_impact,
    
    -- Identify if this is a RUNE trade
    CASE 
        WHEN s.from_asset LIKE 'THOR.RUNE%' OR s.to_asset LIKE 'THOR.RUNE%' THEN true
        ELSE false
    END as involves_rune

FROM swap_events s
-- join asset price (non-RUNE)
LEFT JOIN {{ ref('thorchain_silver_prices') }} fp_asset
    ON fp_asset.contract_address = s.from_contract_address
    AND fp_asset.symbol <> 'RUNE'
    AND fp_asset.block_time <= s.block_time
    AND fp_asset.block_time >= s.block_time - interval '1' hour
-- join RUNE price for from_asset
LEFT JOIN {{ ref('thorchain_silver_prices') }} fp_rune
    ON fp_rune.symbol = 'RUNE'
    AND fp_rune.block_time <= s.block_time
    AND fp_rune.block_time >= s.block_time - interval '1' hour
-- join asset price (non-RUNE) for to_asset
LEFT JOIN {{ ref('thorchain_silver_prices') }} tp_asset
    ON tp_asset.contract_address = s.to_contract_address
    AND tp_asset.symbol <> 'RUNE'
    AND tp_asset.block_time <= s.block_time
    AND tp_asset.block_time >= s.block_time - interval '1' hour
-- join RUNE price for to_asset  
LEFT JOIN {{ ref('thorchain_silver_prices') }} tp_rune
    ON tp_rune.symbol = 'RUNE'
    AND tp_rune.block_time <= s.block_time
    AND tp_rune.block_time >= s.block_time - interval '1' hour

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
    
    -- Calculate USD values
    (s.from_asset_amount / POWER(10,8)) * COALESCE(fp_asset.price, fp_rune.price, 0) as from_amount_usd,
    (s.to_asset_amount / POWER(10,8)) * COALESCE(tp_asset.price, tp_rune.price, 0) as to_amount_usd,
    
    -- Trading metrics
    CASE 
        WHEN COALESCE(fp_asset.price, fp_rune.price, 0) * s.from_asset_amount > 0 
        THEN (COALESCE(tp_asset.price, tp_rune.price, 0) * s.to_asset_amount) / (COALESCE(fp_asset.price, fp_rune.price, 1) * s.from_asset_amount)
        ELSE null 
    END as price_impact,
    
    -- Identify if this is a RUNE trade
    CASE 
        WHEN s.from_asset LIKE 'THOR.RUNE%' OR s.to_asset LIKE 'THOR.RUNE%' THEN true
        ELSE false
    END as involves_rune

FROM streaming_swaps s
-- join asset price (non-RUNE)
LEFT JOIN {{ ref('thorchain_silver_prices') }} fp_asset
    ON fp_asset.contract_address = s.from_contract_address
    AND fp_asset.symbol <> 'RUNE'
    AND fp_asset.block_time <= s.block_time
    AND fp_asset.block_time >= s.block_time - interval '1' hour
-- join RUNE price for from_asset
LEFT JOIN {{ ref('thorchain_silver_prices') }} fp_rune
    ON fp_rune.symbol = 'RUNE'
    AND fp_rune.block_time <= s.block_time
    AND fp_rune.block_time >= s.block_time - interval '1' hour
-- join asset price (non-RUNE) for to_asset
LEFT JOIN {{ ref('thorchain_silver_prices') }} tp_asset
    ON tp_asset.contract_address = s.to_contract_address
    AND tp_asset.symbol <> 'RUNE'
    AND tp_asset.block_time <= s.block_time
    AND tp_asset.block_time >= s.block_time - interval '1' hour
-- join RUNE price for to_asset  
LEFT JOIN {{ ref('thorchain_silver_prices') }} tp_rune
    ON tp_rune.symbol = 'RUNE'
    AND tp_rune.block_time <= s.block_time
    AND tp_rune.block_time >= s.block_time - interval '1' hour
