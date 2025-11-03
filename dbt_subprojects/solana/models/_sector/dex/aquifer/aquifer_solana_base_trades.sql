{{
  config(
    schema = 'aquifer_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-10-30 %} --testing shorter timeframe window, prod: '2025-06-26'

-- Aquifer swaps from instruction_calls table
WITH swaps AS (
    SELECT
          block_slot
        , block_date
        , block_time
        , COALESCE(inner_instruction_index,0) as inner_instruction_index -- adjust to index 0 for direct trades
        , outer_instruction_index
        , inner_executing_account
        , outer_executing_account
        , executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , null AS pool_id -- Aquifer does not use a pool system like other AMM's. Each token has it's own single vault.
    FROM {{ source('solana','instruction_calls') }}
    WHERE 1=1
        AND executing_account = 'AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45' -- aquifer swap program id
        AND BYTEARRAY_SUBSTRING(data, 1, 1) = 0x01 -- Swap tag/discriminator. See: https://dune.com/queries/6025019
        AND tx_success = true 
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_time') }}
        {% else -%}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}
)

-- Join inner token_transfers initiated by amm swap instructions.
-- inner_instruction_index + 1 -> token that trader bought
-- inner_instruction_index + 2 -> token that trader sold
, transfers AS (        
    SELECT
        s.block_date
      , s.block_time
      , s.block_slot
      , CASE 
          WHEN s.is_inner = false THEN 'direct'
          ELSE s.outer_executing_account
        END as trade_source
      , t.amount as token_bought_amount_raw
      , t1.amount as token_sold_amount_raw
      , t.from_token_account as token_bought_vault -- For a list of all Aquifer vaults see: https://dune.com/queries/5855118
      , t1.to_token_account as token_sold_vault
      , t.token_mint_address as token_bought_mint_address
      , t1.token_mint_address as token_sold_mint_address
      , s.pool_id AS project_program_id
      , s.tx_signer as trader_id
      , s.tx_id
      , s.outer_instruction_index
      , s.inner_instruction_index 
      , s.tx_index
    FROM swaps s
    INNER JOIN {{ source('tokens_solana','transfers') }} t  ON t.tx_id = s.tx_id --buy 
        AND t.block_date = s.block_date
        AND t.block_slot = s.block_slot
        AND t.outer_instruction_index = s.outer_instruction_index
        AND t.inner_instruction_index = s.inner_instruction_index + 1
        AND (t.token_version = 'spl_token' or t.token_version = 'spl_token_2022')
        {% if is_incremental() -%}
        AND {{ incremental_predicate('t.block_time') }}
        {% else -%}
        AND t.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}
    INNER JOIN {{ source('tokens_solana','transfers') }} t1  ON t1.tx_id = s.tx_id --sell 
        AND t1.block_date = s.block_date
        AND t1.block_slot = s.block_slot
        AND t1.outer_instruction_index = s.outer_instruction_index
        AND t1.inner_instruction_index = s.inner_instruction_index + 2
        AND (t1.token_version = 'spl_token' or t1.token_version = 'spl_token_2022')
        {% if is_incremental() -%}
        AND {{ incremental_predicate('t1.block_time') }}
        {% else -%}
        AND t1.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}

)

--add pertinent info in prep for union on solana_base_trades
SELECT
      'solana' as blockchain
    , 'aquifer' AS Project
    , 1 AS version
    , 'v1' as version_name
    , date_trunc('month',s.block_date) as block_month
    , s.block_time
    , s.block_slot
    , s.block_date
    , s.trade_source
    , s.token_bought_amount_raw
    , s.token_sold_amount_raw
    , CAST(NULL AS DOUBLE) as fee_tier
    , s.token_bought_mint_address  
    , s.token_sold_mint_address
    , s.token_bought_vault
    , s.token_sold_vault
    , s.project_program_id
    , 'AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45' AS project_main_id
    , s.trader_id 
    , s.tx_id
    , s.outer_instruction_index
    , s.inner_instruction_index
    , s.tx_index
    , {{ dbt_utils.generate_surrogate_key(['tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']) }} as surrogate_key
FROM transfers s
