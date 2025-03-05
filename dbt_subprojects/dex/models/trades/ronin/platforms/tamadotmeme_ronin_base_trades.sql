{{ config(
    schema = 'tamadotmeme_v1_ronin',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

-- Retrieve hourly Ronin prices for the WRON token.
WITH ronin_price AS (
  SELECT
    hour,
    median_price AS ron_price
  FROM {{ source('dex', 'prices') }}
  WHERE contract_address = 0xe514d9deb7966c8be0ca922de8a064264ea6bcd4
),

-- Process "buy" transactions:
-- - Normalizes token amounts (dividing by 10^18).
-- - Joins on token creation events to get a readable token symbol.
-- - Computes the USD value using the hourly Ronin price.
-- - Filters out events before the Tama Trade protocol launch.
buy AS (
  SELECT
    'ronin' AS blockchain,
    'tamadotmeme' AS project,
    1 AS version,
    DATE_TRUNC('month', bet.call_block_time) AS block_month,
    DATE_TRUNC('day', bet.call_block_time) AS block_day,
    bet.call_block_time AS block_time,
    bet.call_block_number AS block_number,
    tc.symbol AS token_bought_symbol, -- Readable token symbol from creation event.
    'WRON' AS token_sold_symbol,       -- WRON represents the wrapped RONIN token.
    cast(bet.output_amountOut as double) / POWER(10, 18) AS token_bough_amount,
    cast(bet.amountIn as double) / POWER(10, 18) AS token_sold_amount,
    cast(bet.output_amountOut as double) AS token_bough_amount_raw,
    cast(bet.amountIn as double) AS token_sold_amount_raw,
    cast(bet.amountIn as double) / POWER(10, 18) * rp.ron_price AS amount_usd,  -- USD value
    bet.token AS token_bought_address,
    0xe514d9deb7966c8be0ca922de8a064264ea6bcd4 AS token_sold_address, -- All tokens on tamadot meme are bought using RONIN
    bet.call_tx_from AS taker,
    bet.contract_address AS maker,
    bet.contract_address AS project_contract_address,
    bet.call_tx_hash AS tx_hash,
    bet.call_tx_from AS tx_from,
    bet.call_tx_to AS tx_to,
    bet.call_tx_index AS evt_index
  FROM  {{ source('tamadotmeme_ronin', 'maincontract_call_buytokenswitheth') }} AS bet
  LEFT JOIN  {{ source('tamadotmeme_ronin', 'maincontract_evt_tokencreated') }} AS tc
    ON bet.token = tc.token
  LEFT JOIN ronin_price AS rp
    ON DATE_TRUNC('hour', bet.call_block_time) = rp.hour
  WHERE call_block_time >= TRY_CAST('2025-02-06 12:23' AS TIMESTAMP)
),

-- Process "sell" transactions:
-- Similar to the "buy" section, this CTE normalizes amounts, adds token symbol info,
-- computes USD value, and applies the same protocol launch filter.
sell AS (
  SELECT
    'ronin' AS blockchain,
    'tamadotmeme' AS project,
    1 AS version,
    DATE_TRUNC('month', ste.call_block_time) AS block_month,
    DATE_TRUNC('day', ste.call_block_time) AS block_day,
    ste.call_block_time AS block_time,
    ste.call_block_number AS block_number,
    'WRON' AS token_bought_symbol,
    tc.symbol AS token_sold_symbol,   -- Readable token symbol from creation event.
    concat(tc.symbol, '-', 'WRON') AS token_pair,
    cast(ste.output_amountOut as double) / POWER(10, 18) AS token_bough_amount,
    cast(ste.amountIn as double) / POWER(10, 18) AS token_sold_amount,
    cast(ste.output_amountOut as double) AS token_bough_amount_raw,
    cast(ste.amountIn as double) AS token_sold_amount_raw,
    cast(ste.output_amountOut as double) / POWER(10, 18) * rp.ron_price AS amount_usd,
    0xe514d9deb7966c8be0ca922de8a064264ea6bcd4 AS token_bought_address,  -- All tokens on tamadot meme are sold for RONIN
    ste.token AS token_sold_address,
    ste.call_tx_from AS taker,
    ste.call_tx_to AS maker,
    ste.contract_address AS project_contract_address,
    ste.call_tx_hash AS tx_hash,
    ste.call_tx_from AS tx_from,
    ste.call_tx_to AS tx_to,
    ste.call_tx_index AS evt_index
  FROM  {{ source('tamadotmeme_ronin', 'maincontract_call_selltokensforeth') }} AS ste
  LEFT JOIN  {{ source('tamadotmeme_ronin', 'maincontract_evt_tokencreated') }} AS tc
    ON ste.token = tc.token
  LEFT JOIN ronin_price AS rp
    ON DATE_TRUNC('hour', ste.call_block_time) = rp.hour
  WHERE call_block_time >= TRY_CAST('2025-02-06 12:23' AS TIMESTAMP)
)

-- Combine buy and sell transactions into one result set.
SELECT * FROM buy
UNION ALL
SELECT * FROM sell
