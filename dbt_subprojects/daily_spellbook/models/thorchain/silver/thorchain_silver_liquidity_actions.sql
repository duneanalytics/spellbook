{{ config(
    schema = 'thorchain_silver',
    alias = 'liquidity_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'tx_hash', 'event_id', 'action_type'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'liquidity', 'curated']
) }}

WITH block_heights AS (
    SELECT DISTINCT
        CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) AS block_time,
        height AS block_id
    FROM {{ source('thorchain','block_log') }}
    WHERE CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) >= current_date - interval '17' day
),

add_events AS (
    SELECT 
        ae.tx_hash,
        ae.block_time,
        ae.block_date,
        ae.block_month,
        ae.block_hour,
        ae.chain,
        ae.from_addr,
        ae.to_addr,
        ae.asset,
        ae.asset_amount,
        ae.asset_e8,
        ae.rune_amount,
        ae.rune_e8,
        ae.memo,
        ae.pool,
        ae.pool_chain,
        ae.pool_asset,
        ae.contract_address,
        ae.event_id,
        bh.block_id,
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
    LEFT JOIN block_heights bh ON ae.block_time = bh.block_time
    WHERE ae.block_time >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('ae.block_time') }}
    {% endif %}
),

withdraw_events AS (
    SELECT 
        we.tx_hash,
        we.block_time,
        we.block_date,
        we.block_month,
        we.block_hour,
        we.chain,
        we.from_addr,
        we.to_addr,
        we.asset,
        we.asset_amount,
        we.asset_e8,
        null as rune_amount,
        null as rune_e8,
        we.memo,
        we.pool,
        we.pool_chain,
        we.pool_asset,
        we.contract_address,
        we.event_id,
        bh.block_id,
        'withdraw' as action_type,
        we.emit_asset_amount,
        we.emit_asset_e8,
        we.emit_rune_amount,
        we.emit_rune_e8,
        we.stake_units,
        we.basis_points,
        we.asymmetry,
        we.imp_loss_protection_amount
    FROM {{ ref('thorchain_silver_withdraw_events') }} we  
    LEFT JOIN block_heights bh ON we.block_time = bh.block_time
    WHERE we.block_time >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('we.block_time') }}
    {% endif %}
)

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
    
    COALESCE(p.asset_usd * COALESCE(a.asset_amount, a.emit_asset_amount), 0) as asset_amount_usd,
    COALESCE(p.rune_usd * COALESCE(a.rune_amount, a.emit_rune_amount), 0) as rune_amount_usd,
    
    COALESCE(p.asset_usd * COALESCE(a.asset_amount, a.emit_asset_amount), 0) + 
    COALESCE(p.rune_usd * COALESCE(a.rune_amount, a.emit_rune_amount), 0) as total_value_usd,
    
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
    ON p.block_id = a.block_id
    AND p.pool_name = a.pool

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
    
    COALESCE(p.asset_usd * COALESCE(w.asset_amount, w.emit_asset_amount), 0) as asset_amount_usd,
    COALESCE(p.rune_usd * COALESCE(w.rune_amount, w.emit_rune_amount), 0) as rune_amount_usd,
    
    COALESCE(p.asset_usd * COALESCE(w.asset_amount, w.emit_asset_amount), 0) + 
    COALESCE(p.rune_usd * COALESCE(w.rune_amount, w.emit_rune_amount), 0) as total_value_usd,
    
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
    ON p.block_id = w.block_id
    AND p.pool_name = w.pool
