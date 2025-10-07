{{ config(
    schema = 'thorchain_silver',
    alias = 'liquidity_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'event_id', 'action_type'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'liquidity', 'curated']
) }}

-- Combine add and withdraw events into a unified liquidity actions view
WITH add_events AS (
    SELECT 
        tx_hash,
        block_time,
        block_date,
        block_month,
        block_hour,
        chain,
        from_addr,
        to_addr,
        asset,
        asset_amount,
        asset_e8,
        rune_amount,
        rune_e8,
        memo,
        pool,
        pool_chain,
        pool_asset,
        contract_address,
        event_id,
        'add' as action_type,
        null as emit_asset_amount,
        null as emit_asset_e8,
        null as emit_rune_amount,
        null as emit_rune_e8,
        null as stake_units,
        null as basis_points,
        null as asymmetry,
        null as imp_loss_protection_amount
    FROM {{ ref('thorchain_silver_add_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
),

withdraw_events AS (
    SELECT 
        tx_hash,
        block_time,
        block_date,
        block_month,
        block_hour,
        chain,
        from_addr,
        to_addr,
        asset,
        asset_amount,
        asset_e8,
        null as rune_amount,
        null as rune_e8,
        memo,
        pool,
        pool_chain,
        pool_asset,
        contract_address,
        event_id,
        'withdraw' as action_type,
        emit_asset_amount,
        emit_asset_e8,
        emit_rune_amount,
        emit_rune_e8,
        stake_units,
        basis_points,
        asymmetry,
        imp_loss_protection_amount
    FROM {{ ref('thorchain_silver_withdraw_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

-- Union add and withdraw events
SELECT 
    tx_hash,
    block_time,
    block_date,
    block_month,
    block_hour,
    chain,
    from_addr,
    to_addr,
    asset,
    asset_amount,
    asset_e8,
    rune_amount,
    rune_e8,
    memo,
    pool,
    pool_chain,
    pool_asset,
    contract_address,
    event_id,
    action_type,
    emit_asset_amount,
    emit_asset_e8,
    emit_rune_amount,
    emit_rune_e8,
    stake_units,
    basis_points,
    asymmetry,
    imp_loss_protection_amount,
    
    -- Calculate USD values by joining with prices
    COALESCE(p.price * COALESCE(asset_amount, emit_asset_amount), 0) as asset_amount_usd,
    COALESCE(rp.rune_price_usd * COALESCE(rune_amount, emit_rune_amount), 0) as rune_amount_usd,
    
    -- Total liquidity action value in USD
    COALESCE(p.price * COALESCE(asset_amount, emit_asset_amount), 0) + 
    COALESCE(rp.rune_price_usd * COALESCE(rune_amount, emit_rune_amount), 0) as total_value_usd,
    
    -- Liquidity action metrics
    CASE 
        WHEN action_type = 'add' AND COALESCE(asset_amount, 0) > 0 AND COALESCE(rune_amount, 0) > 0 THEN 'symmetric_add'
        WHEN action_type = 'add' AND COALESCE(asset_amount, 0) > 0 AND COALESCE(rune_amount, 0) = 0 THEN 'asymmetric_add_asset'
        WHEN action_type = 'add' AND COALESCE(asset_amount, 0) = 0 AND COALESCE(rune_amount, 0) > 0 THEN 'asymmetric_add_rune'
        WHEN action_type = 'withdraw' AND COALESCE(emit_asset_amount, 0) > 0 AND COALESCE(emit_rune_amount, 0) > 0 THEN 'symmetric_withdraw'
        WHEN action_type = 'withdraw' AND COALESCE(emit_asset_amount, 0) > 0 AND COALESCE(emit_rune_amount, 0) = 0 THEN 'asymmetric_withdraw_asset'
        WHEN action_type = 'withdraw' AND COALESCE(emit_asset_amount, 0) = 0 AND COALESCE(emit_rune_amount, 0) > 0 THEN 'asymmetric_withdraw_rune'
        ELSE 'unknown'
    END as liquidity_type

FROM add_events a
LEFT JOIN {{ ref('thorchain_silver_prices') }} p
    ON p.contract_address = a.contract_address
    AND p.block_time <= a.block_time
    AND p.block_time >= a.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_rune_price') }} rp
    ON rp.block_time <= a.block_time
    AND rp.block_time >= a.block_time - interval '1' hour

UNION ALL

SELECT 
    tx_hash,
    block_time,
    block_date,
    block_month,
    block_hour,
    chain,
    from_addr,
    to_addr,
    asset,
    asset_amount,
    asset_e8,
    rune_amount,
    rune_e8,
    memo,
    pool,
    pool_chain,
    pool_asset,
    contract_address,
    event_id,
    action_type,
    emit_asset_amount,
    emit_asset_e8,
    emit_rune_amount,
    emit_rune_e8,
    stake_units,
    basis_points,
    asymmetry,
    imp_loss_protection_amount,
    
    -- Calculate USD values
    COALESCE(p.price * COALESCE(asset_amount, emit_asset_amount), 0) as asset_amount_usd,
    COALESCE(rp.rune_price_usd * COALESCE(rune_amount, emit_rune_amount), 0) as rune_amount_usd,
    
    -- Total liquidity action value in USD
    COALESCE(p.price * COALESCE(asset_amount, emit_asset_amount), 0) + 
    COALESCE(rp.rune_price_usd * COALESCE(rune_amount, emit_rune_amount), 0) as total_value_usd,
    
    -- Liquidity action metrics
    CASE 
        WHEN action_type = 'add' AND COALESCE(asset_amount, 0) > 0 AND COALESCE(rune_amount, 0) > 0 THEN 'symmetric_add'
        WHEN action_type = 'add' AND COALESCE(asset_amount, 0) > 0 AND COALESCE(rune_amount, 0) = 0 THEN 'asymmetric_add_asset'
        WHEN action_type = 'add' AND COALESCE(asset_amount, 0) = 0 AND COALESCE(rune_amount, 0) > 0 THEN 'asymmetric_add_rune'
        WHEN action_type = 'withdraw' AND COALESCE(emit_asset_amount, 0) > 0 AND COALESCE(emit_rune_amount, 0) > 0 THEN 'symmetric_withdraw'
        WHEN action_type = 'withdraw' AND COALESCE(emit_asset_amount, 0) > 0 AND COALESCE(emit_rune_amount, 0) = 0 THEN 'asymmetric_withdraw_asset'  
        WHEN action_type = 'withdraw' AND COALESCE(emit_asset_amount, 0) = 0 AND COALESCE(emit_rune_amount, 0) > 0 THEN 'asymmetric_withdraw_rune'
        ELSE 'unknown'
    END as liquidity_type

FROM withdraw_events w
LEFT JOIN {{ ref('thorchain_silver_prices') }} p
    ON p.contract_address = w.contract_address
    AND p.block_time <= w.block_time
    AND p.block_time >= w.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_rune_price') }} rp
    ON rp.block_time <= w.block_time
    AND rp.block_time >= w.block_time - interval '1' hour
