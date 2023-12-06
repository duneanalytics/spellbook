{{ config(
    
    schema = 'tigris_arbitrum',
    alias = 'options_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trade_type', 'positions_contract', 'protocol_version']
    )
}}

WITH 

open_position as (
    SELECT 
        'arbitrum' as blockchain, 
        op.version, 
        op.protocol_version,
        op.day, 
        op.block_month,
        op.evt_block_time,
        op.evt_tx_hash,
        op.evt_index,
        op.position_id,
        op.open_price,
        op.close_price,
        op.profitnLoss,
        op.collateral as collateral_amount,
        op.collateral as volume_usd,
        op.collateral_asset,
        op.pair,
        op.options_period,
        op.referral,
        op.trader,
        CASE 
            WHEN direction = 'true' THEN 'Buy Options'
            WHEN direction = 'false' THEN 'Sell Options'
        END as trade_direction, 
        'open_position' as trade_type,
        op.positions_contract,
        op.project_contract_address,
        0 as fees 
    FROM 
    {{ ref('tigris_arbitrum_events_options_open_position') }} op 
    WHERE order_type = '0'
    {% if is_incremental() %}
    AND op.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

limit_order as (
    SELECT 
        'arbitrum' as blockchain, 
        op.version, 
        op.protocol_version,
        op.day, 
        op.block_month,
        op.evt_block_time,
        op.evt_tx_hash,
        op.evt_index,
        op.position_id,
        op.open_price,
        op.close_price,
        op.profitnLoss,
        op.collateral as collateral_amount,
        op.collateral as volume_usd,
        op.collateral_asset,
        op.pair,
        op.options_period,
        op.referral,
        op.trader,
        CASE 
            WHEN order_type = '0' AND direction = 'true' THEN 'Buy Options'
            WHEN order_type = '0' AND direction = 'false' THEN 'Sell Options'
            WHEN order_type = '1' AND direction = 'true' THEN 'Limit Buy Options'
            WHEN order_type = '1' AND direction = 'false' THEN 'Limit Sell Options'
            WHEN order_type = '2' AND direction = 'true' THEN 'Buy Stop Options'
            WHEN order_type = '2' AND direction = 'false' THEN 'Sell Stop Options'
        END as trade_direction, 
        'limit_position' as trade_type,
        op.positions_contract,
        op.project_contract_address,
        0 as fees 
    FROM 
    {{ ref('tigris_arbitrum_events_options_limit_order') }} op 
    {% if is_incremental() %}
    WHERE op.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

close_position as (
    SELECT 
        'arbitrum' as blockchain, 
        op.version, 
        op.protocol_version,
        op.day, 
        op.block_month,
        op.evt_block_time,
        op.evt_tx_hash,
        op.evt_index,
        op.position_id,
        op.open_price,
        op.close_price,
        op.profitnLoss,
        op.collateral as collateral_amount,
        CASE WHEN op.profitnLoss > 0 THEN op.profitnLoss ELSE 0 END as volume_usd,
        op.collateral_asset,
        op.pair,
        op.options_period,
        op.referral,
        op.trader,
        CASE 
            WHEN order_type = '0' AND direction = 'true' THEN 'Buy Options'
            WHEN order_type = '0' AND direction = 'false' THEN 'Sell Options'
            WHEN order_type = '1' AND direction = 'true' THEN 'Limit Buy Options'
            WHEN order_type = '1' AND direction = 'false' THEN 'Limit Sell Options'
            WHEN order_type = '2' AND direction = 'true' THEN 'Buy Stop Options'
            WHEN order_type = '2' AND direction = 'false' THEN 'Sell Stop Options'
        END as trade_direction, 
        'close_position' as trade_type,
        op.positions_contract,
        op.project_contract_address,
        f.fees as fees 
    FROM 
    {{ ref('tigris_arbitrum_events_options_close_position') }} op 
    INNER JOIN 
    {{ ref('tigris_arbitrum_events_options_fees_distributed') }} f
        ON op.evt_block_time = f.evt_block_time
        AND op.evt_tx_hash = f.evt_tx_hash
        AND op.evt_join_index = f.evt_join_index
        -- can't join on position id as position id isn't present in fees event so using this workaround 
        {% if is_incremental() %}
        AND f.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    {% if is_incremental() %}
    WHERE op.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

limit_cancel as (
    SELECT 
        'arbitrum' as blockchain, 
        op.version, 
        op.protocol_version,
        op.day, 
        op.block_month,
        op.evt_block_time,
        op.evt_tx_hash,
        op.evt_index,
        op.position_id,
        op.open_price,
        op.close_price,
        op.profitnLoss,
        op.collateral as collateral_amount,
        0 as volume_usd,
        op.collateral_asset,
        op.pair,
        op.options_period,
        op.referral,
        op.trader,
        CASE 
            WHEN order_type = '0' AND direction = 'true' THEN 'Buy Options Cancelled'
            WHEN order_type = '0' AND direction = 'false' THEN 'Sell Options Cancelled'
            WHEN order_type = '1' AND direction = 'true' THEN 'Limit Buy Options Cancelled'
            WHEN order_type = '1' AND direction = 'false' THEN 'Limit Sell Options Cancelled'
            WHEN order_type = '2' AND direction = 'true' THEN 'Buy Stop Options Cancelled'
            WHEN order_type = '2' AND direction = 'false' THEN 'Sell Stop Options Cancelled'
        END as trade_direction, 
        'limit_cancel' as trade_type,
        op.positions_contract,
        op.project_contract_address,
        0 as fees 
    FROM 
    {{ ref('tigris_arbitrum_events_options_limit_cancel') }} op 
    {% if is_incremental() %}
    WHERE op.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

-- use to reload

SELECT * FROM open_position

UNION ALL 

SELECT * FROM limit_order

UNION ALL 

SELECT * FROM close_position

UNION ALL 

SELECT * FROM limit_cancel


