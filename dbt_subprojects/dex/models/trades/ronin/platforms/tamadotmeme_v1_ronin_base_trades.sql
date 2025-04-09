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
    '1' AS version,
    DATE_TRUNC('month', bet.call_block_time) AS block_month,
    DATE_TRUNC('day', bet.call_block_time) AS block_date,
    bet.call_block_time AS block_time,
    bet.call_block_number AS block_number,
    tc.symbol AS token_bought_symbol, -- Readable token symbol from creation event.
    'WRON' AS token_sold_symbol,       -- WRON represents the wrapped RONIN token.
    concat(tc.symbol, '-', 'WRON') AS token_pair,
    cast(bet.output_amountOut as double) / POWER(10, 18) AS token_bought_amount,
    cast(bet.amountIn as double) / POWER(10, 18) AS token_sold_amount,
    cast(bet.output_amountOut as double) AS token_bought_amount_raw,
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
    bet.call_tx_index AS evt_index,
    row_number() over(partition by bet.call_tx_hash order by bet.call_trace_address asc) as rn

  FROM  {{ source('tamadotmeme_ronin', 'maincontract_call_buytokenswitheth') }} AS bet
  LEFT JOIN  {{ source('tamadotmeme_ronin', 'maincontract_evt_tokencreated') }} AS tc
    ON bet.token = tc.token
  LEFT JOIN ronin_price AS rp
    ON DATE_TRUNC('hour', bet.call_block_time) = rp.hour
  WHERE call_block_time >= TRY_CAST('2025-01-21 14:07' AS TIMESTAMP)
  and call_tx_to!=0x9b0a1d03ea99a8b3cf9b7e73e0aa1b805ce45c54 -- edge case where the tx is both a buy and a sell and coincentally the same token has the same event indiex in respective table
  and call_success
),

-- Process "sell" transactions:
-- Similar to the "buy" section, this CTE normalizes amounts, adds token symbol info,
-- computes USD value, and applies the same protocol launch filter.
sell AS (
  SELECT
    'ronin' AS blockchain,
    'tamadotmeme' AS project,
    '1' AS version,
    DATE_TRUNC('month', ste.call_block_time) AS block_month,
    DATE_TRUNC('day', ste.call_block_time) AS block_date,
    ste.call_block_time AS block_time,
    ste.call_block_number AS block_number,
    'WRON' AS token_bought_symbol,
    tc.symbol AS token_sold_symbol,   -- Readable token symbol from creation event.
    concat(tc.symbol, '-', 'WRON') AS token_pair,
    cast(ste.output_amountOut as double) / POWER(10, 18) AS token_bought_amount,
    cast(ste.amountIn as double) / POWER(10, 18) AS token_sold_amount,
    cast(ste.output_amountOut as double) AS token_bought_amount_raw,
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
    ste.call_tx_index AS evt_index,
    row_number() over(partition by ste.call_tx_hash order by ste.call_trace_address asc) as rn

  FROM  {{ source('tamadotmeme_ronin', 'maincontract_call_selltokensforeth') }} AS ste
  LEFT JOIN  {{ source('tamadotmeme_ronin', 'maincontract_evt_tokencreated') }} AS tc
    ON ste.token = tc.token
  LEFT JOIN ronin_price AS rp
    ON DATE_TRUNC('hour', ste.call_block_time) = rp.hour
  WHERE call_block_time >= TRY_CAST('2025-01-21 14:07' AS TIMESTAMP)
  and call_tx_to!=0x9b0a1d03ea99a8b3cf9b7e73e0aa1b805ce45c54 -- edge case where the tx is both a buy and a sell and coincentally the same token has the same event indiex in respective table
  and call_success
)

,combined as 
(
(SELECT * FROM buy where rn=1)
UNION ALL
(SELECT * FROM sell where rn=1)
)
select
  cast (blockchain as varchar) as blockchain
, cast (project as varchar) as project
, cast (version as varchar) as version
, cast (block_month as date) as block_month
, cast (block_date as date) as block_date
, cast (block_time as timestamp) as block_time
, cast (block_number as uint256) as block_number
, cast (token_bought_symbol as varchar) as token_bought_symbol
, cast (token_sold_symbol as varchar) as token_sold_symbol
, cast (token_pair as varchar) as token_pair
, cast (token_bought_amount as double) as token_bought_amount
, cast (token_sold_amount as double) as token_sold_amount
, cast (token_bought_amount_raw as uint256) as token_bought_amount_raw
, cast (token_sold_amount_raw as uint256) as token_sold_amount_raw
, cast (amount_usd as double) as amount_usd
, cast (token_bought_address as varbinary) as token_bought_address
, cast (token_sold_address as varbinary) as token_sold_address
, cast (taker as varbinary) as taker
, cast (maker as varbinary) as maker
, cast (project_contract_address as varbinary) as project_contract_address
, cast (tx_hash as varbinary) as tx_hash
, cast (tx_from as varbinary) as tx_from
, cast (tx_to as varbinary) as tx_to
, cast (evt_index as uint256) as evt_index
from combined 