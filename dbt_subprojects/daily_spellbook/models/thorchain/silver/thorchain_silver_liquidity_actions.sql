{{ config(
    schema = 'thorchain_silver',
    alias = 'liquidity_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'event_id', 'action_type'],
    partition_by = ['block_month'],
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
    FROM {{ ref('thorchain_silver_add_events') }} ae
    WHERE ae.block_time >= current_date - interval '15' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('ae.block_time') }}
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
    FROM {{ ref('thorchain_silver_withdraw_events') }} we  
    WHERE we.block_time >= current_date - interval '15' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('we.block_time') }}
    {% endif %}
)

-- Union add and withdraw events
SELECT 
    a.tx_hash,
    a.block_time,
    a.block_date,
    a.block_month,
    a.block_hour,
    a.chain,
    a.from_addr,
    a.to_addr,
    a.asset,
    a.asset_amount,
    a.asset_e8,
    a.rune_amount,
    a.rune_e8,
    a.memo,
    a.pool,
    a.pool_chain,
    a.pool_asset,
    a.contract_address,
    a.event_id,
    a.action_type,
    a.emit_asset_amount,
    a.emit_asset_e8,
    a.emit_rune_amount,
    a.emit_rune_e8,
    a.stake_units,
    a.basis_points,
    a.asymmetry,
    a.imp_loss_protection_amount,
    
    -- Calculate USD values by joining with prices
    COALESCE(p.price * COALESCE(a.asset_amount, a.emit_asset_amount), 0) as asset_amount_usd,
    COALESCE(rp.rune_price_usd * COALESCE(a.rune_amount, a.emit_rune_amount), 0) as rune_amount_usd,
    
    -- Total liquidity action value in USD
    COALESCE(p.price * COALESCE(a.asset_amount, a.emit_asset_amount), 0) + 
    COALESCE(rp.rune_price_usd * COALESCE(a.rune_amount, a.emit_rune_amount), 0) as total_value_usd,
    
    -- Liquidity action metrics
    CASE 
        WHEN a.action_type = 'add' AND COALESCE(a.asset_amount, 0) > 0 AND COALESCE(a.rune_amount, 0) > 0 THEN 'symmetric_add'
        WHEN a.action_type = 'add' AND COALESCE(a.asset_amount, 0) > 0 AND COALESCE(a.rune_amount, 0) = 0 THEN 'asymmetric_add_asset'
        WHEN a.action_type = 'add' AND COALESCE(a.asset_amount, 0) = 0 AND COALESCE(a.rune_amount, 0) > 0 THEN 'asymmetric_add_rune'
        WHEN a.action_type = 'withdraw' AND COALESCE(a.emit_asset_amount, 0) > 0 AND COALESCE(a.emit_rune_amount, 0) > 0 THEN 'symmetric_withdraw'
        WHEN a.action_type = 'withdraw' AND COALESCE(a.emit_asset_amount, 0) > 0 AND COALESCE(a.emit_rune_amount, 0) = 0 THEN 'asymmetric_withdraw_asset'
        WHEN a.action_type = 'withdraw' AND COALESCE(a.emit_asset_amount, 0) = 0 AND COALESCE(a.emit_rune_amount, 0) > 0 THEN 'asymmetric_withdraw_rune'
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
    w.tx_hash,
    w.block_time,
    w.block_date,
    w.block_month,
    w.block_hour,
    w.chain,
    w.from_addr,
    w.to_addr,
    w.asset,
    w.asset_amount,
    w.asset_e8,
    w.rune_amount,
    w.rune_e8,
    w.memo,
    w.pool,
    w.pool_chain,
    w.pool_asset,
    w.contract_address,
    w.event_id,
    w.action_type,
    w.emit_asset_amount,
    w.emit_asset_e8,
    w.emit_rune_amount,
    w.emit_rune_e8,
    w.stake_units,
    w.basis_points,
    w.asymmetry,
    w.imp_loss_protection_amount,
    
    -- Calculate USD values
    COALESCE(p.price * COALESCE(w.asset_amount, w.emit_asset_amount), 0) as asset_amount_usd,
    COALESCE(rp.rune_price_usd * COALESCE(w.rune_amount, w.emit_rune_amount), 0) as rune_amount_usd,
    
    -- Total liquidity action value in USD
    COALESCE(p.price * COALESCE(w.asset_amount, w.emit_asset_amount), 0) + 
    COALESCE(rp.rune_price_usd * COALESCE(w.rune_amount, w.emit_rune_amount), 0) as total_value_usd,
    
    -- Liquidity action metrics
    CASE 
        WHEN w.action_type = 'add' AND COALESCE(w.asset_amount, 0) > 0 AND COALESCE(w.rune_amount, 0) > 0 THEN 'symmetric_add'
        WHEN w.action_type = 'add' AND COALESCE(w.asset_amount, 0) > 0 AND COALESCE(w.rune_amount, 0) = 0 THEN 'asymmetric_add_asset'
        WHEN w.action_type = 'add' AND COALESCE(w.asset_amount, 0) = 0 AND COALESCE(w.rune_amount, 0) > 0 THEN 'asymmetric_add_rune'
        WHEN w.action_type = 'withdraw' AND COALESCE(w.emit_asset_amount, 0) > 0 AND COALESCE(w.emit_rune_amount, 0) > 0 THEN 'symmetric_withdraw'
        WHEN w.action_type = 'withdraw' AND COALESCE(w.emit_asset_amount, 0) > 0 AND COALESCE(w.emit_rune_amount, 0) = 0 THEN 'asymmetric_withdraw_asset'  
        WHEN w.action_type = 'withdraw' AND COALESCE(w.emit_asset_amount, 0) = 0 AND COALESCE(w.emit_rune_amount, 0) > 0 THEN 'asymmetric_withdraw_rune'
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
