{{ config(
    schema = 'tamadotmeme_v1_ronin',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

{% set project_start_date = '2025-01-21 14:07' %}
{% set wron_token_address = '0xe514d9deb7966c8be0ca922de8a064264ea6bcd4' %}
{% set edge_case_tx_address = '0x9b0a1d03ea99a8b3cf9b7e73e0aa1b805ce45c54' %}


-- Process "buy" transactions:
-- - Normalizes token amounts (dividing by 10^18).
-- - Joins on token creation events to get a readable token symbol.
-- - Computes the USD value using the hourly Ronin price.
-- - Filters out events before the Tama Trade protocol launch.
with buy AS (
  SELECT
    'ronin' AS blockchain,
    'tamadotmeme' AS project,
    '1' AS version,
    DATE_TRUNC('month', bet.call_block_time) AS block_month,
    DATE_TRUNC('day', bet.call_block_time) AS block_date,
    bet.call_block_time AS block_time,
    bet.call_block_number AS block_number,
    cast(bet.output_amountOut as double) AS token_bought_amount_raw,
    cast(bet.amountIn as double) AS token_sold_amount_raw,
    bet.token AS token_bought_address,
    '{{wron_token_address}}' AS token_sold_address, -- All tokens on tamadot meme are bought using RONIN
    bet.call_tx_from AS taker,
    bet.contract_address AS maker,
    bet.contract_address AS project_contract_address,
    bet.call_tx_hash AS tx_hash,
    bet.call_tx_index AS evt_index,
    bet.call_tx_from AS tx_from,
    bet.call_tx_to AS tx_to,
    row_number() over (partition by bet.call_tx_hash, bet.call_tx_index order by bet.call_tx_hash) as duplicates_rank

  FROM  {{ source('tamadotmeme_ronin', 'maincontract_call_buytokenswitheth') }} AS bet
  LEFT JOIN  {{ source('tamadotmeme_ronin', 'maincontract_evt_tokencreated') }} AS tc
    ON bet.token = tc.token
  WHERE call_block_time >= TRY_CAST('{{project_start_date}}' AS TIMESTAMP)
    {% if is_incremental() %}
    AND
        {{ incremental_predicate('bet.call_block_time') }}
    {% endif %}
  and call_tx_to!='{{edge_case_tx_address}}' -- edge case where the tx is both a buy and a sell and coincentally the same token has the same event indiex in respective table
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
    cast(ste.output_amountOut as double) AS token_bought_amount_raw,
    cast(ste.amountIn as double) AS token_sold_amount_raw,
    '{{wron_token_address}}' AS token_bought_address,  -- All tokens on tamadot meme are sold for RONIN
    ste.token AS token_sold_address,
    ste.call_tx_from AS taker,
    ste.call_tx_to AS maker,
    ste.contract_address AS project_contract_address,
    ste.call_tx_hash AS tx_hash,
    ste.call_tx_index AS evt_index,
    ste.call_tx_from AS tx_from,
    ste.call_tx_to AS tx_to,
    row_number() over(partition by ste.call_tx_hash, ste.call_tx_index order by ste.call_trace_address asc) as duplicates_rank

  FROM  {{ source('tamadotmeme_ronin', 'maincontract_call_selltokensforeth') }} AS ste
  LEFT JOIN  {{ source('tamadotmeme_ronin', 'maincontract_evt_tokencreated') }} AS tc
    ON ste.token = tc.token
  WHERE call_block_time >= TRY_CAST('{{project_start_date}}' AS TIMESTAMP)
    {% if is_incremental() %}
    AND
        {{ incremental_predicate('ste.call_block_time') }}
    {% endif %}
  and call_tx_to!='{{edge_case_tx_address}}' -- edge case where the tx is both a buy and a sell and coincentally the same token has the same event indiex in respective table
  and call_success
)

,combined as (

        (SELECT * FROM buy where duplicates_rank=1)
        UNION ALL
        (SELECT * FROM sell where duplicates_rank=1)

)
select
  cast (blockchain as varchar) as blockchain
, cast (project as varchar) as project
, cast (version as varchar) as version
, cast (block_month as date) as block_month
, cast (block_date as date) as block_date
, cast (block_time as timestamp) as block_time
, cast (block_number as uint256) as block_number
, cast (token_bought_amount_raw as uint256) as token_bought_amount_raw
, cast (token_sold_amount_raw as uint256) as token_sold_amount_raw
, cast (token_bought_address as varbinary) as token_bought_address
, cast (token_sold_address as varbinary) as token_sold_address
, cast (taker as varbinary) as taker
, cast (maker as varbinary) as maker
, cast (project_contract_address as varbinary) as project_contract_address
, cast (tx_hash as varbinary) as tx_hash
, cast (evt_index as uint256) as evt_index
, cast (tx_from as varbinary) as tx_from
, cast (tx_to as varbinary) as tx_to
, row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank

from combined