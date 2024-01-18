{{ config(
    
    schema = 'tigris_arbitrum',
    alias = 'trades',
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
        block_month,
        day, 
        project_contract_address,
        evt_block_time, 
        evt_index,
        evt_tx_hash,
        position_id,
        price, 
        margin as new_margin,
        leverage,
        volume_usd,
        margin_asset,
        pair, 
        direction,
        referral,
        trader,
        margin as margin_change, 
        version, 
        'open_position' as trade_type,
        positions_contract,
        protocol_version
    FROM {{ ref('tigris_arbitrum_events_open_position') }}
    WHERE open_type = 'open_position'
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
), 

open_position_join as (
    SELECT 
        block_month,
        day, 
        project_contract_address,
        evt_block_time, 
        evt_index,
        evt_tx_hash,
        position_id,
        price, 
        margin as new_margin,
        leverage,
        volume_usd,
        margin_asset,
        pair, 
        direction,
        referral,
        trader,
        margin as margin_change, 
        version, 
        'open_position' as trade_type,
        positions_contract,
        protocol_version
    FROM {{ ref('tigris_arbitrum_events_open_position') }}
    WHERE open_type = 'open_position'
), 

open_position_limit as (
    SELECT 
        block_month,
        day, 
        project_contract_address,
        evt_block_time, 
        evt_index,
        evt_tx_hash,
        position_id,
        price, 
        margin as new_margin,
        leverage,
        volume_usd,
        margin_asset,
        pair, 
        direction,
        referral,
        trader,
        margin as margin_change, 
        version, 
        'open_position' as trade_type,
        open_type,
        positions_contract,
        protocol_version
    FROM {{ ref('tigris_arbitrum_events_open_position') }}
    WHERE open_type != 'open_position'
),

limit_order as (
    SELECT 
        block_month,
        day, 
        project_contract_address,
        evt_block_time, 
        evt_index,
        evt_tx_hash,
        position_id,
        price, 
        margin as new_margin,
        leverage,
        volume_usd,
        margin_asset,
        pair, 
        direction,
        referral,
        trader,
        margin as margin_change, 
        version, 
        'limit_order' as trade_type,
        positions_contract,
        protocol_version
    FROM {{ ref('tigris_arbitrum_events_limit_order') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
), 

limit_order_join as (
    SELECT 
        block_month,
        day, 
        project_contract_address,
        evt_block_time, 
        evt_index,
        evt_tx_hash,
        position_id,
        price, 
        margin as new_margin,
        leverage,
        volume_usd,
        margin_asset,
        pair, 
        direction,
        referral,
        trader,
        margin as margin_change, 
        version, 
        'limit_order' as trade_type,
        positions_contract,
        protocol_version
    FROM {{ ref('tigris_arbitrum_events_limit_order') }}
), 

close_position as (
    SELECT 
        c.block_month,
        c.day, 
        c.project_contract_address,
        c.evt_block_time,
        c.evt_index,
        c.evt_tx_hash,
        c.position_id,
        c.price, 
        c.new_margin as new_margin, 
        c.leverage, 
        c.payout * c.leverage as volume_usd, 
        COALESCE(op.margin_asset, lo.margin_asset) as margin_asset, 
        COALESCE(op.pair, lo.pair) as pair, 
        COALESCE(op.direction, lo.direction) as direction, 
        COALESCE(op.referral, lo.referral) as referral, 
        c.trader, 
        c.payout as margin_change, 
        c.version, 
        'close_position' as trade_type,
        c.positions_contract,
        c.protocol_version
    FROM 
        {{ ref('tigris_arbitrum_positions_close') }} c
    LEFT JOIN
        open_position_join op 
        ON c.position_id = op.position_id
        AND c.positions_contract = op.positions_contract
        AND c.protocol_version = op.protocol_version
    LEFT JOIN
        limit_order_join lo
        ON c.position_id = lo.position_id
        AND c.positions_contract = lo.positions_contract
        AND c.protocol_version = lo.protocol_version
    {% if is_incremental() %}
    WHERE c.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

), 

liquidate_position as (
    SELECT 
        lp.block_month,
        lp.day, 
        lp.project_contract_address,
        lp.evt_block_time,
        lp.evt_index,
        lp.evt_tx_hash,
        lp.position_id, 
        lp.price,
        0 as new_margin, 
        lp.leverage, 
        lp.margin * lp.leverage as volume_usd, 
        COALESCE(op.margin_asset, lo.margin_asset) as margin_asset, 
        COALESCE(op.pair, lo.pair) as pair, 
        COALESCE(op.direction, lo.direction) as direction, 
        COALESCE(op.referral, lo.referral) as referral, 
        lp.trader, 
        lp.margin as margin_change,
        lp.version, 
        'liquidate_position' as trade_type,
        lp.positions_contract,
        lp.protocol_version
    FROM 
        {{ ref('tigris_arbitrum_positions_liquidation') }} lp 
    LEFT JOIN
        open_position_join op 
        ON lp.position_id = op.position_id 
        AND lp.positions_contract = op.positions_contract
        AND lp.protocol_version = op.protocol_version
    LEFT JOIN
        limit_order_join lo 
        ON lp.position_id = lo.position_id 
        AND lp.positions_contract = lo.positions_contract
        AND lp.protocol_version = lo.protocol_version
    {% if is_incremental() %}
    WHERE lp.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

add_margin as (
    SELECT 
        am.block_month,
        am.day, 
        am.project_contract_address,
        am.evt_block_time,
        am.evt_index,
        am.evt_tx_hash,
        am.position_id,
        am.price,
        am.margin as new_margin,
        am.leverage, 
        am.margin_change * am.leverage as volume_usd,
        COALESCE(op.margin_asset, lo.margin_asset) as margin_asset, 
        COALESCE(op.pair, lo.pair) as pair, 
        COALESCE(op.direction, lo.direction) as direction, 
        COALESCE(op.referral, lo.referral) as referral, 
        am.trader,
        am.margin_change,
        am.version, 
        'add_to_position' as trade_type, 
        am.positions_contract,
        am.protocol_version
    FROM 
    (
        SELECT 
            tmp.*, 
            l.leverage 
        FROM 
        (
            SELECT 
                MIN(l.evt_block_time) as latest_leverage_time, 
                am.block_month,
                am.day, 
                am.evt_block_time,
                am.evt_tx_hash,
                am.evt_index,
                am.position_id,
                am.price, 
                am.margin, 
                am.margin_change,
                am.version,
                am.trader,
                am.project_contract_address,
                am.positions_contract,
                am.protocol_version
            FROM 
            {{ ref('tigris_arbitrum_events_add_margin') }} am
            INNER JOIN 
                {{ ref('tigris_arbitrum_positions_leverage') }} l 
                ON am.position_id = l.position_id 
                AND am.positions_contract = l.positions_contract
                AND am.protocol_version = l.protocol_version
                AND am.evt_block_time > l.evt_block_time
            {% if is_incremental() %}
            WHERE am.evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
            GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        ) tmp 
        INNER JOIN 
            {{ ref('tigris_arbitrum_positions_leverage') }} l 
            ON tmp.position_id = l.position_id
            AND tmp.latest_leverage_time = l.evt_block_time
            AND tmp.protocol_version = l.protocol_version
    ) am  
    LEFT JOIN 
        open_position_join op 
        ON am.position_id = op.position_id 
        AND am.positions_contract = op.positions_contract
        AND am.protocol_version = op.protocol_version
    LEFT JOIN 
        limit_order_join lo 
        ON am.position_id = lo.position_id 
        AND am.positions_contract = lo.positions_contract
        AND am.protocol_version = lo.protocol_version
),

modify_margin as (
    SELECT 
        mm.block_month,
        mm.day, 
        mm.project_contract_address,
        mm.evt_block_time,
        mm.evt_index,
        mm.evt_tx_hash,
        mm.position_id,
        CAST(NULL as double) as price,
        mm.margin as new_margin, 
        mm.leverage,
        mm.margin_change * mm.leverage as volume_usd,
        COALESCE(op.margin_asset, lo.margin_asset) as margin_asset, 
        COALESCE(op.pair, lo.pair) as pair, 
        COALESCE(op.direction, lo.direction) as direction, 
        COALESCE(op.referral, lo.referral) as referral, 
        mm.trader, 
        mm.margin_change,
        mm.version,
        CASE WHEN mm.modify_type = true THEN 'add_margin' ELSE 'remove_margin' END as trade_type,
        mm.positions_contract,
        mm.protocol_version
    FROM 
        {{ ref('tigris_arbitrum_events_modify_margin') }} mm 
    LEFT JOIN 
        open_position_join op 
        ON mm.position_id = op.position_id 
        AND mm.positions_contract = op.positions_contract
        AND mm.protocol_version = op.protocol_version
    LEFT JOIN 
        limit_order_join lo 
        ON mm.position_id = lo.position_id 
        AND mm.positions_contract = lo.positions_contract
        AND mm.protocol_version = lo.protocol_version
    {% if is_incremental() %}
    WHERE mm.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

limit_cancel as (
    SELECT 
        lc.block_month, 
        lc.day, 
        lc.project_contract_address,
        lc.evt_block_time,
        lc.evt_index,
        lc.evt_tx_hash,
        lc.position_id,
        op.price, 
        0 as new_margin,
        op.leverage,
        0 as volume_usd,
        op.margin_asset,
        op.pair, 
        op.direction,
        op.referral,
        lc.trader, 
        0 as margin_change,
        lc.version, 
        COALESCE(CONCAT(op.open_type, ' cancelled'), 'missing-cancelled') as trade_type,
        lc.positions_contract,
        lc.protocol_version
    FROM 
        {{ ref('tigris_arbitrum_events_limit_cancel') }} lc 
    LEFT JOIN 
    open_position_limit op 
        ON lc.position_id = op.position_id 
        AND lc.positions_contract = op.positions_contract
        AND lc.protocol_version = op.protocol_version
    {% if is_incremental() %}
    WHERE lc.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

events_combined as (
    SELECT * FROM open_position

    UNION ALL 

    SELECT * FROM close_position

    UNION ALL 

    SELECT * FROM liquidate_position

    UNION ALL 

    SELECT * FROM add_margin

    UNION ALL 

    SELECT * FROM modify_margin

    UNION ALL 

    SELECT * FROM limit_order

    UNION ALL 

    SELECT * FROM limit_cancel
)

SELECT 
    'arbitrum' as blockchain, 
    block_month,
    day,
    project_contract_address,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    position_id,
    ec.price,
    new_margin,
    CASE
        WHEN margin_asset = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 THEN new_margin * pe.price
        WHEN margin_asset = 0x763e061856b3e74a6c768a859dc2543a56d299d5 THEN new_margin * pe.price
        ELSE new_margin 
    END as new_margin_usd,
    leverage,
    CASE
        WHEN margin_asset = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 THEN volume_usd * pe.price
        WHEN margin_asset = 0x763e061856b3e74a6c768a859dc2543a56d299d5 THEN volume_usd * pe.price
        ELSE volume_usd
    END as volume_usd,
    margin_asset,
    pair,
    direction,
    referral,
    trader,
    margin_change,
    CASE
        WHEN margin_asset = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 THEN margin_change * pe.price
        WHEN margin_asset = 0x763e061856b3e74a6c768a859dc2543a56d299d5 THEN margin_change * pe.price
        ELSE margin_change
    END as margin_change_usd,
    version,
    trade_type,
    positions_contract,
    protocol_version
FROM 
events_combined ec 
LEFT JOIN {{ source('prices', 'usd') }} pe 
    ON pe.minute = date_trunc('minute', ec.evt_block_time)
    AND pe.blockchain = 'arbitrum'
    AND pe.symbol = 'WETH'
    {% if is_incremental() %}
    AND pe.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}