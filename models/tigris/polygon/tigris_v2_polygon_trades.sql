{{ config(
    tags=['dunesql'],
    schema = 'tigris_v2_polygon',
    alias = alias('trades'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trade_type']
    )
}}

WITH 

open_position as (
    SELECT 
        day, 
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
        'open_position' as trade_type 
    FROM {{ ref('tigris_v2_polygon_events_open_position') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
), 

limit_order as (
    SELECT 
        day, 
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
        'limit_order' as trade_type 
    FROM {{ ref('tigris_v2_polygon_events_limit_order') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
), 

close_position as (
    SELECT 
        TRY_CAST(date_trunc('DAY', c.evt_block_time) AS date) as day, 
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
        'close_position' as trade_type 
    FROM 
        {{ ref('tigris_v2_polygon_positions_close') }} c
    LEFT JOIN
        open_position op 
        ON c.position_id = op.position_id
    LEFT JOIN
        limit_order lo
        ON c.position_id = lo.position_id
    {% if is_incremental() %}
    WHERE c.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

), 

liquidate_position as (
    SELECT 
        lp.day, 
        lp.evt_block_time,
        lp.evt_index,
        lp.evt_tx_hash,
        lp.position_id, 
        CAST(NULL as double) as price, 
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
        'liquidate_position' as trade_type
    FROM 
        {{ ref('tigris_v2_polygon_positions_liquidation') }} lp 
    LEFT JOIN
        open_position op 
        ON lp.position_id = op.position_id 
    LEFT JOIN
        limit_order lo 
        ON lp.position_id = lo.position_id 
    {% if is_incremental() %}
    WHERE lp.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

add_margin as (
    SELECT 
        am.day, 
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
        'add_to_position' as trade_type 
    FROM 
    (
        SELECT 
            tmp.*, 
            l.leverage 
        FROM 
        (
            SELECT 
                MIN(l.evt_block_time) as latest_leverage_time, 
                am.day, 
                am.evt_block_time,
                am.evt_tx_hash,
                am.evt_index,
                am.position_id,
                am.price, 
                am.margin, 
                am.margin_change,
                am.version,
                am.trader
            FROM 
                {{ ref('tigris_v2_polygon_events_add_margin') }} am 
            INNER JOIN 
                {{ ref('tigris_v2_polygon_positions_leverage') }} l 
                ON am.position_id = l.position_id 
                AND am.evt_block_time > l.evt_block_time
                {% if is_incremental() %}
                AND l.evt_block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %}
            {% if is_incremental() %}
            WHERE am.evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
            GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        ) tmp 
        INNER JOIN 
            {{ ref('tigris_v2_polygon_positions_leverage') }} l 
            ON tmp.position_id = l.position_id
            AND tmp.latest_leverage_time = l.evt_block_time
            {% if is_incremental() %}
            AND l.evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
    ) am  
    LEFT JOIN 
        open_position op 
        ON am.position_id = op.position_id 
    LEFT JOIN 
        limit_order lo 
        ON am.position_id = lo.position_id 
),

modify_margin as (
    SELECT 
        mm.day, 
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
        CASE WHEN mm.modify_type = true THEN 'add_margin' ELSE 'remove_margin' END as trade_type
    FROM 
        {{ ref('tigris_v2_polygon_events_modify_margin') }} mm 
    LEFT JOIN 
        open_position op 
        ON mm.position_id = op.position_id 
    LEFT JOIN 
        limit_order lo 
        ON mm.position_id = lo.position_id 
    {% if is_incremental() %}
    WHERE mm.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT 
    'polygon' as blockchain, 
    * 
FROM open_position

UNION ALL

SELECT 
    'polygon' as blockchain,
    *
FROM close_position

UNION ALL

SELECT 
    'polygon' as blockchain, 
    * 
FROM liquidate_position

UNION ALL

SELECT 
    'polygon' as blockchain,
    *
FROM add_margin

UNION ALL

SELECT 
    'polygon' as blockchain,
    *
FROM modify_margin

UNION ALL 

SELECT 
    'polygon' as blockchain,
    *
FROM limit_order
