{{ config(
    
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "tigris",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-09-13' %}

WITH 

fees_open_position_v1 as (
    SELECT 
        *, 
        CASE 
            WHEN UPPER(pair) NOT IN ('EUR/USD', 'GBP/USD', 'CAD/USD', 'JPY/USD', 'RUB/USD') THEN (volume_usd * 0.1)/100
            WHEN UPPER(pair) IN ('EUR/USD', 'GBP/USD', 'CAD/USD', 'JPY/USD', 'RUB/USD') THEN (volume_usd * 0.02)/100
            ELSE 0 
        END as fees 
    FROM 
    {{ ref('tigris_arbitrum_trades') }}
    WHERE protocol_version = '1'
    AND trade_type = 'open_position'
    AND version IN ('v1.2', 'v1.3')
),

fees_close_position_v1 as (
    SELECT 
        c.*, 
        COALESCE((CASE 
            WHEN UPPER(c.pair) NOT IN ('EUR/USD', 'GBP/USD', 'CAD/USD', 'JPY/USD', 'RUB/USD') THEN (o.volume_usd * 0.1)/100 * (c.price/o.price)
            WHEN UPPER(c.pair) IN ('EUR/USD', 'GBP/USD', 'CAD/USD', 'JPY/USD', 'RUB/USD') THEN (o.volume_usd * 0.02)/100 * (c.price/o.price)
            ELSE 0 
        END), 0) as fees 
    FROM 
    {{ ref('tigris_arbitrum_trades') }} c 
    LEFT JOIN 
    {{ ref('tigris_arbitrum_trades') }} o 
        ON c.position_id = o.position_id
        AND c.positions_contract = o.positions_contract
        AND o.trade_type = 'open_position'
        AND o.protocol_version = '1'
    WHERE c.protocol_version = '1'
    AND c.trade_type = 'close_position'
    AND c.version IN ('v1.2', 'v1.3')
), 

excluded_trades as (
    SELECT 
        *, 
        0 as fees 
    FROM 
    {{ ref('tigris_arbitrum_trades') }}
    WHERE protocol_version = '1'
    AND trade_type NOT IN ('open_position', 'close_position')
    AND version IN ('v1.2', 'v1.3')
), 

trades_with_fees_event as (
    SELECT 
        t.*, 
        COALESCE(f.fees, 0) as fees 
    FROM 
    {{ ref('tigris_arbitrum_trades') }} t 
    LEFT JOIN 
    {{ ref('tigris_arbitrum_events_fees_distributed') }} f 
        ON t.evt_block_time = f.evt_block_time
        AND t.evt_tx_hash = f.evt_tx_hash
    WHERE t.version NOT IN ('v1.2', 'v1.3')
),

all_fees as (

    SELECT * FROM trades_with_fees_event

    UNION ALL 

    SELECT * FROM fees_open_position_v1

    UNION ALL 

    SELECT * FROM fees_close_position_v1

    UNION ALL 

    SELECT * FROM excluded_trades
)
-- use to reload 

SELECT 
    t.blockchain, 
    t.day as block_date, 
    t.block_month,
    t.evt_block_time as block_time, 
    CASE 
        WHEN SUBSTRING(pair, 4, 1) = '/' AND SUBSTRING(pair, 5, 3) IN ('USD', 'usd') THEN SUBSTRING(pair, 1, 3) 
        WHEN SUBSTRING(pair, 4, 1) = '/' AND SUBSTRING(pair, 5, 3) NOT IN ('USD', 'usd') THEN SUBSTRING(pair, 5, 3) 
        WHEN SUBSTRING(pair, 5, 1 ) = '/' AND SUBSTRING(pair, 6, 3) IN ('USD', 'usd') THEN SUBSTRING(pair, 1, 4 )
        WHEN pair = 'MATIC/USD' THEN  'MATIC'
        WHEN pair = 'LINK/BTC' THEN 'LINK'
    END as virtual_asset,
    er.symbol as underlying_asset,
    t.pair as market, 
    t.project_contract_address as market_address,
    t.volume_usd,
    CASE
        WHEN margin_asset = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 THEN t.fees * pe.price
        WHEN margin_asset = 0x763e061856b3e74a6c768a859dc2543a56d299d5 THEN t.fees * pe.price
        ELSE t.fees
    END fee_usd,
    t.margin_change_usd as margin_usd,
    CONCAT(t.trade_type, '_', COALESCE((CASE WHEN t.direction = 'true' THEN 'long' WHEN t.direction = 'false' THEN 'short' END), 'Unspecified')) as trade, 
    'tigris_trade' as project, 
    t.version, 
    'tigris_trade' as frontend, 
    t.trader, 
    CAST((volume_usd * 1e18) as UINT256) as volume_raw,
    t.evt_tx_hash as tx_hash, 
    tx."from" as tx_from, 
    tx.to as tx_to, 
    t.evt_index,
    t.protocol_version,
    t.position_id,
    t.positions_contract
FROM 
all_fees t 
INNER JOIN 
{{ source('arbitrum', 'transactions') }} tx
    ON t.evt_tx_hash = tx.hash
    AND t.evt_block_time = tx.block_time
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN 
{{ source('tokens_arbitrum', 'erc20') }} er 
    ON t.margin_asset = er.contract_address
LEFT JOIN {{ source('prices', 'usd') }} pe 
    ON pe.minute = date_trunc('minute', t.evt_block_time)
    AND pe.blockchain = 'arbitrum'
    AND pe.symbol = 'WETH'
    {% if is_incremental() %}
    AND pe.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}




