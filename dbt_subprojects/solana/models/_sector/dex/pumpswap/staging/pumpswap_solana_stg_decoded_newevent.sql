{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'stg_decoded_newevent'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2026-03-06' %}

    -- New buy event decoded from decoded table 
    WITH new_buy_events_raw AS (
        SELECT
              g.evt_block_time
            , g.evt_block_slot
            , g.evt_block_date
            , g.evt_outer_instruction_index
            , g.evt_inner_instruction_index
            , g.evt_tx_id
            , g.evt_tx_index
            , g.evt_outer_executing_account
            , g.pool
            , g.user 
            , g.user_base_token_account AS account_user_base_token_account
            , g.user_quote_token_account AS account_user_quote_token_account
            , f.account_arguments[8] AS account_pool_base_token_account  
            , f.account_arguments[9] AS account_pool_quote_token_account  
            , g.protocol_fee_recipient_token_account AS account_protocol_fee_recipient_token_account
            , g.base_amount_out AS base_amount
            , g.quote_amount_in  AS quote_amount
            , ROW_NUMBER() OVER (
                PARTITION BY g.evt_tx_id, g.evt_outer_instruction_index, g.evt_inner_instruction_index 
                ORDER BY f.inner_instruction_index
              ) AS rn    -- To avoid potential duplicate rows rom the join
        FROM {{ source('pumpdotfun_solana', 'pump_amm_evt_buyevent') }} g
        INNER JOIN {{ source('solana', 'instruction_calls') }} f 
            ON g.evt_tx_id = f.tx_id
            AND g.evt_block_date = f.block_date
            AND g.evt_outer_instruction_index = f.outer_instruction_index
            AND bytearray_substring(f.data, 1, 8) = 0xc62e1552b4d9e870
            AND f.executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
            AND f.tx_success = true
            AND f.block_time >= TIMESTAMP '{{ project_start_date }}'
            {% if is_incremental() %}
            AND {{ incremental_predicate('f.block_time') }}
            {% endif %}
        WHERE 1=1
            {% if is_incremental() %}
            AND {{ incremental_predicate('g.evt_block_time') }}
            {% else %}
            AND g.evt_block_time >= TIMESTAMP '{{ project_start_date }}'
            {% endif %}


    )

    SELECT
          evt_block_slot AS block_slot
        , CAST(date_trunc('month', evt_block_date) AS DATE) AS block_month
        , evt_block_date AS block_date
        , evt_block_time as block_time
        , COALESCE(evt_inner_instruction_index, 0) AS inner_instruction_index
        , evt_inner_instruction_index AS swap_inner_index
        , evt_outer_instruction_index AS outer_instruction_index
        , evt_outer_executing_account AS outer_executing_account
        , evt_tx_id AS tx_id
        , evt_tx_index AS tx_index
        , pool
        , user as user_account
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount
        , quote_amount
        , 1 AS is_buy
        , {{ solana_instruction_key(
          'evt_block_slot'
        , 'evt_tx_index'
        , 'evt_outer_instruction_index'
        , 'COALESCE(evt_inner_instruction_index, 0)'
      ) }} AS surrogate_key
    FROM new_buy_events_raw
    WHERE rn = 1