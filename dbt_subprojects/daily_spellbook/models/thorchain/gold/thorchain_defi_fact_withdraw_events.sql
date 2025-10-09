{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_withdraw_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'withdraw_events', 'fact']
) }}

-- DeFi fact table for raw Thorchain withdraw events
-- Provides access to detailed withdrawal event data
SELECT
    we.tx_hash,
    we.block_time,
    b.block_date,
    b.block_month,
    b.block_hour,
    b.height as block_height,
    
    -- Transaction details
    we.chain,
    we.from_addr as liquidity_provider,
    we.to_addr as recipient,
    we.memo,
    we.tx_type,
    
    -- Asset details (what was originally deposited)
    we.asset as deposited_token_symbol,
    we.asset_amount as deposited_token_amount,
    we.asset_e8 as deposited_token_amount_raw,
    we.contract_address as deposited_token_address,
    
    -- Emitted asset details (what was withdrawn)
    we.emit_asset_amount as withdrawn_token_amount,
    we.emit_asset_e8 as withdrawn_token_amount_raw,
    we.emit_rune_amount as withdrawn_rune_amount,
    we.emit_rune_e8 as withdrawn_rune_amount_raw,
    
    -- Pool details
    we.pool,
    we.pool_chain,
    we.pool_asset,
    
    -- Withdrawal specifics
    we.stake_units,
    we.basis_points / 10000.0 as withdrawal_percent,
    we.asymmetry,
    we.imp_loss_protection_amount,
    we.imp_loss_protection_e8,
    we.emit_asset_in_rune_amount,
    we.emit_asset_in_rune_e8,
    
    -- USD values
    COALESCE(ap.price * we.asset_amount, 0) as deposited_amount_usd,
    COALESCE(ep.price * we.emit_asset_amount, 0) as withdrawn_asset_amount_usd, 
    COALESCE(rp.rune_price_usd * we.emit_rune_amount, 0) as withdrawn_rune_amount_usd,
    COALESCE(ep.price * we.emit_asset_amount, 0) + COALESCE(rp.rune_price_usd * we.emit_rune_amount, 0) as total_withdrawn_usd,
    COALESCE(rp.rune_price_usd * we.imp_loss_protection_amount, 0) as imp_loss_protection_usd,
    
    -- DEX aggregator fields for compatibility
    'thorchain' as project,
    '1' as version,
    'liquidity_withdraw' as category,
    we.event_id,
    
    -- Withdrawal analysis
    CASE 
        WHEN we.emit_asset_amount > 0 AND we.emit_rune_amount > 0 THEN 'symmetric_withdraw'
        WHEN we.emit_asset_amount > 0 AND COALESCE(we.emit_rune_amount, 0) = 0 THEN 'asymmetric_withdraw_asset'
        WHEN COALESCE(we.emit_asset_amount, 0) = 0 AND we.emit_rune_amount > 0 THEN 'asymmetric_withdraw_rune' 
        ELSE 'unknown'
    END as withdrawal_type,
    
    -- Withdrawal completeness
    CASE 
        WHEN we.basis_points >= 10000 THEN 'full_withdrawal'
        WHEN we.basis_points > 0 THEN 'partial_withdrawal'
        ELSE 'unknown'
    END as withdrawal_completeness,
    
    -- Impermanent loss protection flags
    CASE WHEN we.imp_loss_protection_amount > 0 THEN true ELSE false END as has_imp_loss_protection,
    
    -- Asset vs RUNE proportion in withdrawal
    CASE 
        WHEN COALESCE(rp.rune_price_usd * we.emit_rune_amount, 0) > 0 
        THEN COALESCE(ep.price * we.emit_asset_amount, 0) / COALESCE(rp.rune_price_usd * we.emit_rune_amount, 1)
        ELSE null
    END as withdrawn_asset_rune_ratio

FROM {{ ref('thorchain_silver_withdraw_events') }} we
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
    ON we.block_time >= b.block_time
    AND we.block_time < b.block_time + interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} ap
    ON ap.contract_address = we.contract_address
    AND ap.block_time <= we.block_time
    AND ap.block_time >= we.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} ep
    ON ep.contract_address = we.contract_address
    AND ep.block_time <= we.block_time
    AND ep.block_time >= we.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_rune_price') }} rp
    ON rp.block_time <= we.block_time
    AND rp.block_time >= we.block_time - interval '1' hour

{% if is_incremental() %}
WHERE {{ incremental_predicate('we.block_time') }}
  AND we.block_time >= current_date - interval '7' day
{% endif %}
