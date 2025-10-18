{{
  config(
    schema = 'myriad_abstract',
    alias = 'market_trades_raw',
    materialized = 'view'
  )
}}

WITH market_actions as (
SELECT *, CASE WHEN action = 0 THEN 'buy' ELSE 'sell' END as direction, 'abstract' as blockchain FROM {{ source('myriad_abstract', 'predictionmarketv3_evt_marketactiontx') }}
WHERE action IN (0,1)
UNION ALL 
SELECT *, CASE WHEN action = 0 THEN 'buy' ELSE 'sell' END as direction, 'abstract' as blockchain FROM {{ source('myriad_abstract', 'predictionmarketv3_3_points_evt_marketactiontx') }}
WHERE action IN (0,1)
UNION ALL 
SELECT *, CASE WHEN action = 0 THEN 'buy' ELSE 'sell' END as direction, 'abstract' as blockchain FROM {{ source('myriad_abstract', 'predictionmarketv4_evt_marketactiontx') }}
WHERE action IN (0,1)
),

markets as (
SELECT * FROM {{ ref('myriad_abstract_markets') }}
),

tagged_trades as (
SELECT *, row_number() OVER () AS row_number FROM market_actions
),

latest_market_state_for_trade AS (
SELECT t.row_number,
max(m.block_time) AS latest_update_time
FROM tagged_trades t
LEFT JOIN markets m
ON m.contract_address = t.contract_address
AND m.marketId = t.marketId
AND m.block_time <= t.evt_block_time
GROUP BY 1
),

trades as (
SELECT 
t.contract_address,
evt_tx_hash as tx_hash,
evt_tx_from as tx_from,
evt_tx_to as tx_to,
evt_tx_index as tx_index,
evt_index as evt_index,
evt_block_time as block_time,
evt_block_number as block_number,
evt_block_date as block_date,
action,
t.marketId,
outcomeId,
shares,
timestamp,
user,
value,
direction,
t.blockchain,
collateral_token,
question,
buy_fee,
sell_fee,
points
FROM tagged_trades t
LEFT JOIN latest_market_state_for_trade l
ON l.row_number = t.row_number
LEFT JOIN markets m
ON m.contract_address = t.contract_address
AND m.marketId = t.marketId
AND m.block_time = l.latest_update_time
)

SELECT t.*, 
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
FROM trades t
LEFT JOIN prices.usd p ON t.collateral_token = CAST(p.contract_address AS VARCHAR) AND t.blockchain = p.blockchain AND DATE_TRUNC('minute', t.evt_block_time) = p.minute
WHERE (p.blockchain = 'abstract' OR p.blockchain IS NULL)
