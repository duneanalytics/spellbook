{{
  config(
        schema = 'jupiterz_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
        unique_key = ['block_month', 'block_date', 'surrogate_key'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}')
}}

{% set project_start_date = '2024-10-10' %} --grabbed program deployed at time (account created at)

WITH fills AS (
    SELECT
          call_block_time AS block_time
        , call_block_slot AS block_slot
        , CAST(date_trunc('month', call_block_date) AS DATE) AS block_month
        , call_block_date AS block_date
        , CASE
            WHEN call_is_inner = false THEN 'direct'
            ELSE call_outer_executing_account
          END AS trade_source
        , input_amount AS token_sold_amount_raw
        , output_amount AS token_bought_amount_raw
        , account_input_mint AS token_sold_mint_address
        , account_output_mint AS token_bought_mint_address
        , account_maker_input_mint_token_account AS token_sold_vault
        , account_maker_output_mint_token_account AS token_bought_vault
        , account_taker AS trader_id
        , call_outer_instruction_index AS outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) AS inner_instruction_index
        , call_tx_index AS tx_index
        , call_tx_id AS tx_id
        , {{ solana_instruction_key(
              'call_block_slot'
            , 'call_tx_index'
            , 'call_outer_instruction_index'
            , 'call_inner_instruction_index'
          ) }} AS surrogate_key
    FROM {{ source('jupiter_solana','order_engine_call_fill') }}
    WHERE 1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('call_block_date') }}
        {% else -%}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
)

SELECT
      'solana' AS blockchain
    , 'jupiterz' AS project
    , 1 AS version
    , 'v1' AS version_name
    , block_month
    , block_time
    , block_slot
    , block_date
    , trade_source
    , token_bought_amount_raw
    , token_sold_amount_raw
    , CAST(NULL AS DOUBLE) AS fee_tier
    , token_bought_mint_address
    , token_sold_mint_address
    , token_bought_vault
    , token_sold_vault
    , CAST(NULL AS VARCHAR) AS project_program_id
    , '61DFfeTKM7trxYcPQCM78bJ794ddZprZpAwAnLiwTpYH' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
    , surrogate_key
FROM fills
