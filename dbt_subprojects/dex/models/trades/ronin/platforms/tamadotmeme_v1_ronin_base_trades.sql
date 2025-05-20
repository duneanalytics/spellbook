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


with trades AS (
    select 
    'ronin' AS blockchain, -- Tag the blockchain
    'tamadotmeme' AS project, -- Tag the project
    '1' AS version, -- Set a version
    CAST(DATE_TRUNC('month', evt_block_time) as date) AS block_month, -- Extract month from block time
    CAST(DATE_TRUNC('day', evt_block_time) as date) AS block_date, -- Extract day from block time
    evt_block_time AS block_time, -- Keep original block time
    evt_block_number AS block_number, -- Keep block number
    case when isBuy=True then cast(amountOut as uint256)  -- If it's a buy, amountOut is the bought amount
         else cast(amountIn as uint256)                 -- Otherwise, amountIn is the bought amount
    end AS token_bought_amount_raw,
    case when isBuy=True then cast(amountIn as uint256)  -- If it's a buy, amountIn is the sold amount
         else cast(amountOut as uint256)                 -- Otherwise, amountOut is the sold amount
    end AS token_sold_amount_raw,
    case when isBuy=True then token                    -- If it's a buy, the 'token' column holds bought token
         else {{wron_token_address}}                  -- Otherwise, it's wRON
    end as token_bought_address, 
    case when isBuy=True then {{wron_token_address}}      -- If it's a buy, wRON is the sold token
         else token                                    -- Otherwise, the 'token' column holds sold token
    end as token_sold_address, -- All tokens on tamadot meme are bought using RONIN
    case when isBuy=True then sender                   -- If buy, sender is taker
         else to                                       -- Otherwise, receiver is taker
    end as taker,
    case when isBuy=True then to                       -- If buy, receiver is maker
         else sender                                   -- Otherwise, sender is maker
    end as maker,
    contract_address AS project_contract_address, -- Keep contract address
    evt_tx_hash AS tx_hash, -- Keep transaction hash
    evt_index, -- Keep event index
    evt_tx_from AS tx_from, -- Keep transaction sender
    evt_tx_to AS tx_to  -- Keep transaction receiver
  FROM  {{ source('tamadotmeme_ronin', 'maincontract_evt_trade') }} -- Source table
  WHERE evt_block_time >= TRY_CAST('{{project_start_date}}' AS TIMESTAMP) -- Filter by project start date
    {% if is_incremental() %}
    AND
        {{ incremental_predicate('evt_block_time') }} -- Incremental load predicate
    {% endif %}
)

select
*
from trades
