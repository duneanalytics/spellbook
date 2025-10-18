{{
  config(
    schema = 'myriad_linea',
    alias = 'market_trades_raw',
    materialized = 'view'
  )
}}

WITH market_actions as (
SELECT *, CASE WHEN action = 0 THEN 'buy' ELSE 'sell' END as direction, 'linea' as blockchain FROM myriad_linea.predictionmarketv3_4_evt_marketactiontx
WHERE action IN (0,1)
),

markets as (
SELECT * FROM {{ ref('myriad_linea_markets') }}
)

SELECT 
ma.*, 
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
