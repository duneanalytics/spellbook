{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'stg_decoded_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , on_schema_change = 'sync_all_columns'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-02-20' %}
{% set event_start_date = '2025-11-01' %}

WITH event_only_swaps AS (
    SELECT
          e.evt_block_time
        , e.evt_block_slot
        , e.evt_block_date
        , e.evt_outer_instruction_index
        , e.evt_inner_instruction_index
        , e.evt_tx_id
        , e.evt_tx_index
        , e.evt_outer_executing_account
        , e.pool
        , e.user
        , e.user_base_token_account
        , e.user_quote_token_account
        , e.protocol_fee_recipient_token_account
        , e.base_amount_out
        , e.quote_amount_in
    FROM {{ source('pumpdotfun_solana', 'pump_amm_evt_buyevent') }} e
    LEFT JOIN {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }} c
        ON c.call_block_date = e.evt_block_date
        AND c.call_tx_id = e.evt_tx_id
        AND c.call_outer_instruction_index = e.evt_outer_instruction_index
        AND c.account_pool = e.pool
        AND c.base_amount_out = e.base_amount_out
        {% if is_incremental() %}
        AND {{ incremental_predicate('c.call_block_date') }}
        {% else %}
        AND c.call_block_date >= DATE '{{ event_start_date }}'
        {% endif %}
    WHERE c.call_tx_id IS NULL
        {% if is_incremental() %}
        AND {{ incremental_predicate('e.evt_block_date') }}
        {% else %}
        AND e.evt_block_date >= DATE '{{ event_start_date }}'
        {% endif %}
)

, pool_accounts AS (
    SELECT
          account_pool AS pool
        , arbitrary(account_pool_base_token_account) AS pool_base_token_account
        , arbitrary(account_pool_quote_token_account) AS pool_quote_token_account
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_create_pool') }}
    WHERE call_block_time >= TIMESTAMP '{{ project_start_date }}'
    GROUP BY 1
)

, event_swaps AS (
    SELECT
          e.evt_block_time AS call_block_time
        , e.evt_block_slot AS call_block_slot
        , e.evt_block_date AS call_block_date
        , e.evt_outer_instruction_index AS call_outer_instruction_index
        , e.evt_inner_instruction_index AS call_inner_instruction_index
        , e.evt_tx_id AS call_tx_id
        , e.evt_tx_index AS call_tx_index
        , e.evt_outer_executing_account AS call_outer_executing_account
        , e.pool AS account_pool
        , e.user AS account_user
        , e.user_base_token_account AS account_user_base_token_account
        , e.user_quote_token_account AS account_user_quote_token_account
        , p.pool_base_token_account AS account_pool_base_token_account
        , p.pool_quote_token_account AS account_pool_quote_token_account
        , e.protocol_fee_recipient_token_account AS account_protocol_fee_recipient_token_account
        , e.base_amount_out AS base_amount
        , e.quote_amount_in AS quote_amount
        , 1 AS is_buy
    FROM event_only_swaps e
    INNER JOIN pool_accounts p
        ON p.pool = e.pool
)

, swaps AS (
    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_tx_index
        , call_outer_executing_account
        , account_pool
        , account_user
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount_out AS base_amount
        , CAST(NULL AS UINT256) AS quote_amount
        , 1 AS is_buy
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_tx_index
        , call_outer_executing_account
        , account_pool
        , account_user
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount_in AS base_amount
        , CAST(NULL AS UINT256) AS quote_amount
        , 0 AS is_buy
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_tx_index
        , call_outer_executing_account
        , account_pool
        , account_user
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount
        , quote_amount
        , 1 AS is_buy
    FROM event_swaps
)

SELECT
      sp.call_block_slot AS block_slot
    , CAST(date_trunc('month', sp.call_block_date) AS DATE) AS block_month
    , sp.call_block_date AS block_date
    , sp.call_block_time AS block_time
    , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
    , sp.call_inner_instruction_index AS swap_inner_index
    , sp.call_outer_instruction_index AS outer_instruction_index
    , sp.call_outer_executing_account AS outer_executing_account
    , sp.call_tx_id AS tx_id
    , sp.call_tx_index AS tx_index
    , sp.account_pool AS pool
    , sp.account_user AS user_account
    , sp.account_user_base_token_account
    , sp.account_user_quote_token_account
    , sp.account_pool_base_token_account
    , sp.account_pool_quote_token_account
    , sp.account_protocol_fee_recipient_token_account
    , sp.base_amount
    , sp.quote_amount
    , sp.is_buy
    , {{ solana_instruction_key(
          'sp.call_block_slot'
        , 'sp.call_tx_index'
        , 'sp.call_outer_instruction_index'
        , 'COALESCE(sp.call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM swaps sp
