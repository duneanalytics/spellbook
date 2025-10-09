{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_fee_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'fee_events', 'fact']
) }}

-- DeFi fact table for Thorchain fee events
-- Provides comprehensive fee and revenue analytics
SELECT
    fe.tx_hash,
    fe.block_time,
    b.block_date,
    b.block_month,
    b.block_hour,
    b.height as block_height,
    
    -- Asset details
    fe.asset as fee_token_symbol,
    fe.asset_amount as fee_token_amount,
    fe.asset_e8 as fee_token_amount_raw,
    fe.contract_address as fee_token_address,
    fe.asset_chain,
    fe.asset_symbol,
    
    -- Pool deduction details
    fe.pool_deduct_amount,
    fe.pool_deduct as pool_deduct_raw,
    
    -- USD values
    COALESCE(p.price * fe.asset_amount, 0) as fee_amount_usd,
    COALESCE(p.price * fe.pool_deduct_amount, 0) as pool_deduct_usd,
    
    -- DEX aggregator fields for compatibility
    'thorchain' as project,
    '1' as version,
    'fee' as category,
    fe.event_id,
    
    -- Fee analysis
    CASE 
        WHEN fe.asset LIKE 'THOR.RUNE%' THEN 'rune_fee'
        WHEN fe.asset LIKE 'BTC.%' THEN 'btc_fee'
        WHEN fe.asset LIKE 'ETH.%' THEN 'eth_fee'
        WHEN fe.asset LIKE 'BSC.%' THEN 'bsc_fee'
        WHEN fe.asset LIKE 'BNB.%' THEN 'bnb_fee'
        WHEN fe.asset LIKE 'DOGE.%' THEN 'doge_fee'
        ELSE 'other_fee'
    END as fee_category,
    
    -- Fee source classification
    CASE 
        WHEN fe.pool_deduct_amount > fe.asset_amount THEN 'pool_rebalancing'
        WHEN fe.pool_deduct_amount = fe.asset_amount THEN 'standard_fee'
        WHEN fe.pool_deduct_amount < fe.asset_amount THEN 'fee_addition'
        ELSE 'unknown'
    END as fee_type,
    
    -- Revenue metrics for protocol analysis
    COALESCE(p.price * fe.asset_amount, 0) as protocol_revenue_usd,
    
    -- Network fee vs liquidity provider fee approximation
    CASE 
        WHEN fe.pool_deduct_amount > 0 THEN COALESCE(p.price * fe.pool_deduct_amount, 0)
        ELSE 0
    END as estimated_lp_fee_usd,
    
    CASE 
        WHEN fe.asset_amount > fe.pool_deduct_amount 
        THEN COALESCE(p.price * (fe.asset_amount - fe.pool_deduct_amount), 0)
        ELSE 0
    END as estimated_network_fee_usd,
    
    -- Fee efficiency metrics
    CASE 
        WHEN fe.asset_amount > 0 THEN fe.pool_deduct_amount / fe.asset_amount
        ELSE 0
    END as pool_fee_ratio

FROM {{ ref('thorchain_silver_fee_events') }} fe
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
    ON fe.block_time >= b.block_time
    AND fe.block_time < b.block_time + interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} p
    ON p.contract_address = fe.contract_address
    AND p.block_time <= fe.block_time
    AND p.block_time >= fe.block_time - interval '1' hour
WHERE fe.block_time >= current_date - interval '7' day
{% if is_incremental() %}
  AND {{ incremental_predicate('fe.block_time') }}
{% endif %}
