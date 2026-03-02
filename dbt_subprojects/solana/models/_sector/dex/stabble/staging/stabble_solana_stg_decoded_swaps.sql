{{
  config(
    schema = 'stabble_solana'
    , alias = 'stg_decoded_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2024-04-01' %}

WITH swaps AS (
    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_id
        , call_tx_index
        , amount_in
        , account_pool
        , CAST(NULL AS VARCHAR) AS token_sold_mint_address
        , CAST(NULL AS VARCHAR) AS token_bought_mint_address
        , account_user_token_in
        , account_user_token_out
        , account_vault_token_in
        , account_vault_token_out
    FROM {{ source('stable_swap_solana', 'stable_swap_call_swap') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_id
        , call_tx_index
        , amount_in
        , account_pool
        , account_mint_in AS token_sold_mint_address
        , account_mint_out AS token_bought_mint_address
        , account_user_token_in
        , account_user_token_out
        , account_vault_token_in
        , account_vault_token_out
    FROM {{ source('stable_swap_solana', 'stable_swap_call_swap_v2') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_id
        , call_tx_index
        , amount_in
        , account_pool
        , CAST(NULL AS VARCHAR) AS token_sold_mint_address
        , CAST(NULL AS VARCHAR) AS token_bought_mint_address
        , account_user_token_in
        , account_user_token_out
        , account_vault_token_in
        , account_vault_token_out
    FROM {{ source('stable_swap_solana', 'weighted_swap_call_swap') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_id
        , call_tx_index
        , amount_in
        , account_pool
        , account_mint_in AS token_sold_mint_address
        , account_mint_out AS token_bought_mint_address
        , account_user_token_in
        , account_user_token_out
        , account_vault_token_in
        , account_vault_token_out
    FROM {{ source('stable_swap_solana', 'weighted_swap_call_swap_v2') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

SELECT
      sp.call_block_slot AS block_slot
    , CAST(date_trunc('month', sp.call_block_date) AS DATE) AS block_month
    , sp.call_block_date AS block_date
    , sp.call_block_time AS block_time
    , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
    , sp.call_outer_instruction_index AS outer_instruction_index
    , sp.call_outer_executing_account AS outer_executing_account
    , sp.call_is_inner AS is_inner
    , sp.call_tx_id AS tx_id
    , sp.call_tx_signer AS tx_signer
    , sp.call_tx_index AS tx_index
    , sp.account_pool AS pool_id
    , sp.amount_in AS token_sold_amount_raw
    , sp.token_sold_mint_address
    , sp.token_bought_mint_address
    , sp.account_user_token_in AS user_token_in
    , sp.account_user_token_out AS user_token_out
    , sp.account_vault_token_in AS token_sold_vault
    , sp.account_vault_token_out AS token_bought_vault
    , {{ solana_instruction_key(
          'sp.call_block_slot'
        , 'sp.call_tx_index'
        , 'sp.call_outer_instruction_index'
        , 'COALESCE(sp.call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM swaps sp
