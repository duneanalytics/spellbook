{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_swaps',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'swaps', 'fact']
) }}

-- DeFi fact table for Thorchain swaps
-- Final analytical layer with enriched swap data
SELECT
    s.tx_hash,
    s.block_time,
    b.block_date,
    b.block_month,
    b.block_hour,
    b.height as block_height,
    
    -- Transaction details
    s.chain,
    s.from_addr as trader,
    s.to_addr as recipient,
    s.memo,
    
    -- From asset details
    s.from_asset as token_sold_symbol,
    s.from_asset_amount as token_sold_amount,
    s.from_e8 as token_sold_amount_raw,
    s.from_contract_address as token_sold_address,
    s.from_amount_usd as amount_usd_sold,
    
    -- To asset details  
    s.to_asset as token_bought_symbol,
    s.to_asset_amount as token_bought_amount,
    s.to_e8 as token_bought_amount_raw,
    s.to_contract_address as token_bought_address,
    s.to_amount_usd as amount_usd_bought,
    
    -- Pool and trading details
    s.pool,
    s.pool_chain,
    s.pool_asset,
    s.to_e8_min_amount as min_amount_out,
    s.swap_slip_bp / 10000.0 as slippage_percent,
    s.liq_fee_amount,
    s.liq_fee_in_rune_amount,
    
    -- Streaming swap details
    s.streaming,
    s.streaming_count,
    s.streaming_quantity,
    
    -- Trading metrics
    s.price_impact,
    s.involves_rune,
    
    -- Calculate trading fees in USD
    COALESCE(rp.rune_price_usd * s.liq_fee_in_rune_amount, 0) as trading_fee_usd,
    
    -- DEX aggregator fields for compatibility
    'thorchain' as project,
    '1' as version,
    'AMM' as category,
    s.event_id,
    s.source_table,
    
    -- Volume and value metrics
    GREATEST(s.from_amount_usd, s.to_amount_usd) as volume_usd,
    
    -- Trade direction relative to RUNE
    CASE
        WHEN s.from_asset LIKE 'THOR.RUNE%' THEN 'sell_rune'
        WHEN s.to_asset LIKE 'THOR.RUNE%' THEN 'buy_rune'
        ELSE 'asset_to_asset'
    END as trade_direction

FROM {{ ref('thorchain_silver_swaps') }} s
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
    ON s.block_time >= b.block_time
    AND s.block_time < b.block_time + interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_rune_price') }} rp
    ON rp.block_time <= s.block_time
    AND rp.block_time >= s.block_time - interval '1' hour

{% if is_incremental() %}
WHERE {{ incremental_predicate('s.block_time') }}
  AND s.block_time >= current_date - interval '7' day
{% endif %}
