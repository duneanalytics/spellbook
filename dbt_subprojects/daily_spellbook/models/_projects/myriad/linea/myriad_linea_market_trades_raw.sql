{{
  config(
    schema = 'myriad_linea',
    alias = 'market_trades_raw',
    materialized = 'view'
  )
}}

WITH market_actions as (
SELECT *, CASE WHEN action = 0 THEN 'buy' ELSE 'sell' END as direction, 'linea' as blockchain FROM {{ source('myriad_linea', 'predictionmarketv3_4_evt_marketactiontx') }}
WHERE action IN (0,1)
),

markets as (
SELECT * FROM {{ ref('myriad_linea_markets') }}
)

SELECT 
ma.contract_address,
evt_tx_hash as tx_hash,
evt_tx_from as tx_from,
evt_tx_to as tx_to,
evt_tx_index as tx_index,
evt_index as evt_index,
evt_block_time as block_time,
evt_block_number as block_number,
evt_block_date as block_date,
action,
ma.marketId,
outcomeId,
shares,
timestamp,
user,
value,
direction,
ma.blockchain,
collateral_token,
question,
buy_fee,
sell_fee,
points,
value/POW(10, decimals)*price as amount_usd,
CASE
WHEN direction = 'buy' THEN value/POW(10, decimals)*buy_fee
WHEN direction = 'sell' THEN value/POW(10, decimals)*sell_fee
ELSE 0 END as fee,
CASE
WHEN direction = 'buy' THEN value/POW(10, decimals)*buy_fee*price
WHEN direction = 'sell' THEN value/POW(10, decimals)*sell_fee*price
ELSE 0 END as fee_usd,
price as collateral_token_price,
decimals as collateral_token_decimals
FROM market_actions ma
LEFT JOIN markets m ON ma.contract_address = m.contract_address AND ma.marketId = m.marketId
LEFT JOIN prices.usd p ON m.collateral_token = CAST(p.contract_address AS VARCHAR) AND ma.blockchain = p.blockchain AND DATE_TRUNC('minute', ma.evt_block_time) = p.minute
WHERE (p.blockchain = 'linea' OR p.blockchain IS NULL)
