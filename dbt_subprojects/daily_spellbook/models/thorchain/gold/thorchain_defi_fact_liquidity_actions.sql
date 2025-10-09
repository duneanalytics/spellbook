{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_liquidity_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'event_id', 'action_type'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'liquidity', 'fact']
) }}

-- DeFi fact table for Thorchain liquidity actions (adds and withdrawals)
-- Provides comprehensive liquidity provision analytics
SELECT
    la.tx_hash,
    la.block_time,
    b.block_date,
    b.block_month,
    b.block_hour,
    b.height as block_height,
    
    -- Transaction details
    la.chain,
    la.from_addr as liquidity_provider,
    la.to_addr as recipient,
    la.memo,
    
    -- Action details
    la.action_type,
    la.liquidity_type,
    
    -- Asset details
    la.asset as token_symbol,
    la.asset_amount as token_amount,
    la.asset_e8 as token_amount_raw,
    la.contract_address as token_address,
    la.asset_amount_usd as token_amount_usd,
    
    -- RUNE details
    la.rune_amount,
    la.rune_e8 as rune_amount_raw, 
    la.rune_amount_usd,
    
    -- Pool details
    la.pool,
    la.pool_chain,
    la.pool_asset,
    
    -- Withdrawal specific fields
    la.emit_asset_amount,
    la.emit_asset_e8,
    la.emit_rune_amount,
    la.emit_rune_e8,
    la.stake_units,
    la.basis_points / 10000.0 as withdrawal_percent,
    la.asymmetry,
    la.imp_loss_protection_amount,
    
    -- Total liquidity value
    la.total_value_usd,
    
    -- DEX aggregator fields for compatibility
    'thorchain' as project,
    '1' as version,
    'liquidity' as category,
    la.event_id,
    
    -- Liquidity metrics
    CASE 
        WHEN la.action_type = 'add' THEN la.total_value_usd
        ELSE 0
    END as liquidity_added_usd,
    
    CASE 
        WHEN la.action_type = 'withdraw' THEN la.total_value_usd
        ELSE 0
    END as liquidity_removed_usd,
    
    -- Asset vs RUNE ratio for analysis
    CASE 
        WHEN la.rune_amount_usd > 0 THEN la.asset_amount_usd / la.rune_amount_usd
        ELSE null
    END as asset_rune_ratio,
    
    -- Liquidity provision patterns
    CASE 
        WHEN la.liquidity_type LIKE '%symmetric%' THEN 'symmetric'
        WHEN la.liquidity_type LIKE '%asymmetric%' THEN 'asymmetric'
        ELSE 'unknown'
    END as provision_style,
    
    -- Add/withdraw flags for easy filtering
    CASE WHEN la.action_type = 'add' THEN true ELSE false END as is_add,
    CASE WHEN la.action_type = 'withdraw' THEN true ELSE false END as is_withdraw,
    
    -- Impermanent loss protection flag
    CASE 
        WHEN la.imp_loss_protection_amount > 0 THEN true
        ELSE false
    END as has_imp_loss_protection

FROM {{ ref('thorchain_silver_liquidity_actions') }} la
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
    ON la.block_time >= b.block_time
    AND la.block_time < b.block_time + interval '1' hour
    AND b.block_time >= current_date - interval '7' day
WHERE la.block_time >= current_date - interval '7' day
{% if is_incremental() %}
  AND {{ incremental_predicate('la.block_time') }}
{% endif %}
