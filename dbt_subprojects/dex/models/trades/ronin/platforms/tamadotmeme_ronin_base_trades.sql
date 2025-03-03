{{ config(
    schema = 'tamadotmeme_v1_ronin'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH ronin_price AS (
  SELECT
    hour , median_price as ron_price from dex.prices 
    where contract_address=0xe514d9deb7966c8be0ca922de8a064264ea6bcd4
),
buy as 
(
SELECT
  'ronin' AS blockchain,
  'tamadotmeme' AS project,
  1 AS version,
  DATE_TRUNC('month', bet.call_block_time) AS block_month,
  DATE_TRUNC('day', bet.call_block_time) AS block_day,
  bet.call_block_time AS block_time,
  bet.call_block_number AS block_number,
  tc.symbol AS token_bought_symbol,
  'WRON' AS token_sold_symbol,
  concat(tc.symbol,'-','WRON') AS token_pair,
  bet.output_amountOut / POWER(10, 18) AS token_bough_amount,
  bet.amountIn / POWER(10, 18) AS token_sold_amount,
  bet.output_amountOut AS token_bough_amount_raw,
  bet.amountIn AS token_sold_amount_raw,
  bet.amountIn / POWER(10, 18) * rp.ron_price AS amount_usd, 
  bet.token AS token_bought_address,
  0xe514d9deb7966c8be0ca922de8a064264ea6bcd4 AS token_sold_address,
  bet.call_tx_from AS taker,
  bet.contract_address AS maker,
  bet.contract_address AS project_contract_address,
  bet.call_tx_hash AS tx_hash,
  bet.call_tx_from AS tx_from,
  bet.call_tx_to AS tx_to,
  bet.call_tx_index AS evt_index
FROM tamadotmeme_ronin.maincontract_call_buytokenswitheth AS bet
LEFT JOIN tamadotmeme_ronin.maincontract_evt_tokencreated AS tc
  ON bet.token = tc.token
LEFT JOIN ronin_price AS rp
  ON DATE_TRUNC('hour', bet.call_block_time) = rp.hour
WHERE
  1 = 1
  -- AND call_tx_from = 0x468201e260fead6c7553529355f522d14f0e569e
  AND call_block_time >= TRY_CAST('2025-02-06 12:23' AS TIMESTAMP)
),
sell as 
(

SELECT
  'ronin' AS blockchain,
  'tamadotmeme' AS project,
  1 AS version,
  DATE_TRUNC('month', ste.call_block_time) AS block_month,
  DATE_TRUNC('day', ste.call_block_time) AS block_day,
  ste.call_block_time AS block_time,
  ste.call_block_number AS block_number,
  'WRON' AS token_bought_symbol,
  tc.symbol AS token_sold_symbol,
  concat(tc.symbol,'-','WRON') AS token_pair,
  ste.output_amountOut / POWER(10, 18) AS token_bough_amount,
  ste.amountIn / POWER(10, 18) AS token_sold_amount,
  ste.output_amountOut AS token_bough_amount_raw,
  ste.amountIn AS token_sold_amount_raw,
  ste.output_amountOut / POWER(10, 18) * rp.ron_price AS amount_usd, 
  0xe514d9deb7966c8be0ca922de8a064264ea6bcd4 AS token_bought_address,
  ste.token as token_sold_address,
  ste.call_tx_from AS taker,
  ste.call_tx_to AS maker,
  ste.contract_address AS project_contract_address,
  ste.call_tx_hash AS tx_hash,
  ste.call_tx_from AS tx_from,
  ste.call_tx_to AS tx_to,
  ste.call_tx_index AS evt_index
FROM tamadotmeme_ronin.maincontract_call_selltokensforeth AS ste
LEFT JOIN tamadotmeme_ronin.maincontract_evt_tokencreated AS tc
  ON ste.token = tc.token
LEFT JOIN ronin_price AS rp
  ON DATE_TRUNC('hour', ste.call_block_time) = rp.hour
WHERE
  1 = 1
  -- AND call_tx_from = 0x468201e260fead6c7553529355f522d14f0e569e
  AND call_block_time >= TRY_CAST('2025-02-06 12:23' AS TIMESTAMP)
),
combined as 
(
select * from buy 
union all 
select * from sell 
)
select * from combined where tx_from=0x468201e260fead6c7553529355f522d14f0e569e
