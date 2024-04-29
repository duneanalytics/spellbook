{{ config(
    
    schema = 'tigris',
    alias = 'trades_pnl',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trade_type', 'positions_contract', 'protocol_version'],
    post_hook='{{ expose_spells(\'["arbitrum", "polygon"]\',
                                "project",
                                "tigris",
                                \'["Henrystats"]\') }}'
    )
}}

WITH 

margin as (
    SELECT *, 'arbitrum' as blockchain FROM {{ ref('tigris_arbitrum_positions_margin') }}

    UNION ALL 
    
    SELECT *, 'polygon' as blockchain FROM {{ ref('tigris_polygon_positions_margin') }}
),

close_position_tmp as (
    SELECT *, 'arbitrum' as blockchain FROM {{ ref('tigris_arbitrum_events_close_position') }}
    
    UNION ALL 
    
    SELECT *, 'polygon' as blockchain FROM {{ ref('tigris_polygon_events_close_position') }}
),


liquidate_position_tmp as (
    SELECT *, 'arbitrum' as blockchain FROM {{ ref('tigris_arbitrum_events_liquidate_position') }}
    
    UNION ALL 
    
    SELECT *, 'polygon' as blockchain FROM {{ ref('tigris_polygon_events_liquidate_position') }}
),

last_margin as (
        SELECT 
            xx.evt_block_time,
            xx.position_id,
            xx.positions_contract,
            xx.blockchain,
            xy.margin
        FROM 
        (
        SELECT 
            MAX(evt_block_time) as evt_block_time,
            position_id,
            positions_contract,
            blockchain
        FROM 
        margin
        GROUP BY 2, 3, 4 
        ) xx 
        INNER JOIN 
        margin xy 
            ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
            AND xx.positions_contract = xy.positions_contract
            AND xx.blockchain = xy.blockchain
),

close_position as (
        SELECT 
            tmp.evt_block_time, 
            tmp.position_id, 
            tmp.evt_tx_hash, 
            tmp.blockchain,
            tmp.payout - (tmp.perc_closed/100 * (lm.margin)) as pnl 
        FROM 
        (
        SELECT 
            MAX(lm.evt_block_time) as latest_margin_time, 
            cp.evt_block_time, 
            cp.position_id,
            cp.evt_tx_hash, 
            cp.payout, 
            cp.perc_closed,
            cp.positions_contract,
            cp.blockchain
        FROM 
        close_position_tmp cp 
        INNER JOIN 
        margin lm 
            ON cp.position_id = lm.position_id
            AND cp.positions_contract = lm.positions_contract
            AND cp.blockchain = lm.blockchain
            AND cp.evt_block_time > lm.evt_block_time
        GROUP BY 2, 3, 4, 5, 6, 7, 8 
        ) tmp 
        INNER JOIN 
        margin lm 
            ON tmp.position_id = lm.position_id
            AND tmp.latest_margin_time = lm.evt_block_time
            AND tmp.blockchain = lm.blockchain
            AND tmp.positions_contract = lm.positions_contract
), 

liquidate_position as (
        SELECT 
            -lm.margin as pnl, 
            lp.evt_tx_hash, 
            lp.position_id, 
            lp.evt_block_time,
            lp.blockchain
        FROM 
        liquidate_position_tmp lp 
        INNER JOIN 
        last_margin lm 
            ON lp.position_id = lm.position_id
            AND lp.positions_contract = lm.positions_contract
            AND lp.blockchain = lm.blockchain
), 

all_pnl as (
        SELECT 
            evt_block_time, 
            pnl,
            blockchain,
            evt_tx_hash, 
            position_id
        FROM 
        close_position
        
        UNION ALL 
        
        SELECT 
            evt_block_time, 
            pnl,
            blockchain,
            evt_tx_hash,
            position_id
        FROM 
        liquidate_position
), 

close_liquidate as (
    SELECT 
        * 
    FROM 
    {{ ref('tigris_trades') }}
    WHERE trade_type IN ('close_position', 'liquidate_position')
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

close_liquidate_pnl as (
    SELECT 
        cl.*, 
        CASE
            WHEN margin_asset = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 AND cl.blockchain = 'arbitrum' THEN p.pnl * pe.price
            WHEN margin_asset = 0x763e061856b3e74a6c768a859dc2543a56d299d5 AND cl.blockchain = 'arbitrum' THEN p.pnl * pe.price
            ELSE p.pnl
        END as pnl,
        CASE 
            WHEN p.pnl > 0 THEN 1 ELSE 0 END as wins, 
        CASE 
            WHEN p.pnl <= 0 THEN 1 ELSE 0 END as losses
    FROM 
    close_liquidate cl 
    LEFT JOIN 
    all_pnl p 
        ON cl.position_id = p.position_id
        AND cl.evt_tx_hash = p.evt_tx_hash 
        AND cl.blockchain = p.blockchain
    LEFT JOIN {{ source('prices', 'usd') }} pe 
        ON pe.minute = date_trunc('minute', cl.evt_block_time)
        AND pe.blockchain = 'arbitrum'
        AND pe.symbol = 'WETH'
        {% if is_incremental() %}
        AND {{ incremental_predicate('pe.minute') }}
        {% endif %}
)
-- use to reload x
SELECT * FROM close_liquidate_pnl
