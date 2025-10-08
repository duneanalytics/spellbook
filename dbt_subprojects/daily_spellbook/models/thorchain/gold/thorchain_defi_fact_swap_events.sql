{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_swap_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'swap_events', 'fact']
) }}

-- DeFi fact table for raw Thorchain swap events
-- Provides access to raw swap event data for detailed analysis
SELECT
    se.tx_hash,
    se.block_time,
    b.block_date,
    b.block_month,
    b.block_hour,
    b.height as block_height,
    
    -- Transaction details
    se.chain,
    se.from_addr as trader,
    se.to_addr as recipient,
    se.memo,
    
    -- From asset details
    se.from_asset as token_sold_symbol,
    se.from_asset_amount as token_sold_amount,
    se.from_e8 as token_sold_amount_raw,
    se.from_contract_address as token_sold_address,
    
    -- To asset details
    se.to_asset as token_bought_symbol,
    se.to_asset_amount as token_bought_amount,
    se.to_e8 as token_bought_amount_raw,
    se.to_contract_address as token_bought_address,
    
    -- Pool and trading details
    se.pool,
    se.pool_chain,
    se.pool_asset,
    se.to_e8_min_amount as min_amount_out,
    se.swap_slip_bp / 10000.0 as slippage_percent,
    se.liq_fee_amount,
    se.liq_fee_in_rune_amount,
    se.direction,
    
    -- Streaming swap details
    se.streaming,
    se.streaming_count,
    se.streaming_quantity,
    se.tx_type,
    
    -- USD values
    COALESCE(fp.price * se.from_asset_amount, 0) as amount_usd_sold,
    COALESCE(tp.price * se.to_asset_amount, 0) as amount_usd_bought,
    COALESCE(rp.rune_price_usd * se.liq_fee_in_rune_amount, 0) as trading_fee_usd,
    
    -- DEX aggregator fields for compatibility
    'thorchain' as project,
    '1' as version,
    'AMM' as category,
    se.event_id,
    
    -- Trading metrics
    CASE 
        WHEN COALESCE(fp.price * se.from_asset_amount, 0) > 0 
        THEN COALESCE(tp.price * se.to_asset_amount, 0) / COALESCE(fp.price * se.from_asset_amount, 1)
        ELSE null 
    END as price_impact,
    
    -- Volume and value metrics
    GREATEST(COALESCE(fp.price * se.from_asset_amount, 0), COALESCE(tp.price * se.to_asset_amount, 0)) as volume_usd,
    
    -- Trade direction relative to RUNE
    CASE
        WHEN se.from_asset LIKE 'THOR.RUNE%' THEN 'sell_rune'
        WHEN se.to_asset LIKE 'THOR.RUNE%' THEN 'buy_rune'
        ELSE 'asset_to_asset'
    END as trade_direction

FROM {{ ref('thorchain_silver_swap_events') }} se
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
    ON se.block_time >= b.block_time
    AND se.block_time < b.block_time + interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} fp
    ON fp.contract_address = se.from_contract_address
    AND fp.block_time <= se.block_time
    AND fp.block_time >= se.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} tp
    ON tp.contract_address = se.to_contract_address
    AND tp.block_time <= se.block_time
    AND tp.block_time >= se.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_rune_price') }} rp
    ON rp.block_time <= se.block_time
    AND rp.block_time >= se.block_time - interval '1' hour

{% if is_incremental() %}
WHERE {{ incremental_predicate('se.block_time') }}
{% endif %}
