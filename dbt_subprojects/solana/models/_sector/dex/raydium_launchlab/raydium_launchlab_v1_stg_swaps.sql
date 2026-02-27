{{
  config(
    schema = 'raydium_launchlab_v1'
    , alias = 'stg_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-04-15' %}

WITH calls AS (
    SELECT
          sp.call_block_slot AS block_slot
        , CAST(date_trunc('month', sp.call_block_time) AS DATE) AS block_month
        , CAST(date_trunc('day', sp.call_block_time) AS DATE) AS block_date
        , sp.call_block_time AS block_time
        , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
        , sp.call_outer_instruction_index AS outer_instruction_index
        , sp.call_outer_executing_account AS outer_executing_account
        , sp.call_is_inner AS is_inner
        , sp.call_tx_id AS tx_id
        , sp.call_tx_signer AS tx_signer
        , sp.call_tx_index AS tx_index
        , sp.account_pool_state AS pool_id
        , sp.account_base_token_mint AS base_token_mint
        , sp.account_quote_token_mint AS quote_token_mint
        , sp.account_base_vault AS base_vault
        , sp.account_quote_vault AS quote_vault
        , sp.account_platform_config AS platform_config
        , 1 AS is_buy
    FROM {{ source('raydium_solana', 'raydium_launchpad_call_buy_exact_in') }} sp
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('sp.call_block_time') }}
        {% else %}
        AND sp.call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND sp.call_block_time < TIMESTAMP '2025-04-22'
        {% endif %}

    UNION ALL

    SELECT
          sp.call_block_slot AS block_slot
        , CAST(date_trunc('month', sp.call_block_time) AS DATE) AS block_month
        , CAST(date_trunc('day', sp.call_block_time) AS DATE) AS block_date
        , sp.call_block_time AS block_time
        , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
        , sp.call_outer_instruction_index AS outer_instruction_index
        , sp.call_outer_executing_account AS outer_executing_account
        , sp.call_is_inner AS is_inner
        , sp.call_tx_id AS tx_id
        , sp.call_tx_signer AS tx_signer
        , sp.call_tx_index AS tx_index
        , sp.account_pool_state AS pool_id
        , sp.account_base_token_mint AS base_token_mint
        , sp.account_quote_token_mint AS quote_token_mint
        , sp.account_base_vault AS base_vault
        , sp.account_quote_vault AS quote_vault
        , sp.account_platform_config AS platform_config
        , 1 AS is_buy
    FROM {{ source('raydium_solana', 'raydium_launchpad_call_buy_exact_out') }} sp
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('sp.call_block_time') }}
        {% else %}
        AND sp.call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND sp.call_block_time < TIMESTAMP '2025-04-22'
        {% endif %}

    UNION ALL

    SELECT
          sp.call_block_slot AS block_slot
        , CAST(date_trunc('month', sp.call_block_time) AS DATE) AS block_month
        , CAST(date_trunc('day', sp.call_block_time) AS DATE) AS block_date
        , sp.call_block_time AS block_time
        , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
        , sp.call_outer_instruction_index AS outer_instruction_index
        , sp.call_outer_executing_account AS outer_executing_account
        , sp.call_is_inner AS is_inner
        , sp.call_tx_id AS tx_id
        , sp.call_tx_signer AS tx_signer
        , sp.call_tx_index AS tx_index
        , sp.account_pool_state AS pool_id
        , sp.account_base_token_mint AS base_token_mint
        , sp.account_quote_token_mint AS quote_token_mint
        , sp.account_base_vault AS base_vault
        , sp.account_quote_vault AS quote_vault
        , sp.account_platform_config AS platform_config
        , 0 AS is_buy
    FROM {{ source('raydium_solana', 'raydium_launchpad_call_sell_exact_in') }} sp
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('sp.call_block_time') }}
        {% else %}
        AND sp.call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND sp.call_block_time < TIMESTAMP '2025-04-22'
        {% endif %}

    UNION ALL

    SELECT
          sp.call_block_slot AS block_slot
        , CAST(date_trunc('month', sp.call_block_time) AS DATE) AS block_month
        , CAST(date_trunc('day', sp.call_block_time) AS DATE) AS block_date
        , sp.call_block_time AS block_time
        , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
        , sp.call_outer_instruction_index AS outer_instruction_index
        , sp.call_outer_executing_account AS outer_executing_account
        , sp.call_is_inner AS is_inner
        , sp.call_tx_id AS tx_id
        , sp.call_tx_signer AS tx_signer
        , sp.call_tx_index AS tx_index
        , sp.account_pool_state AS pool_id
        , sp.account_base_token_mint AS base_token_mint
        , sp.account_quote_token_mint AS quote_token_mint
        , sp.account_base_vault AS base_vault
        , sp.account_quote_vault AS quote_vault
        , sp.account_platform_config AS platform_config
        , 0 AS is_buy
    FROM {{ source('raydium_solana', 'raydium_launchpad_call_sell_exact_out') }} sp
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('sp.call_block_time') }}
        {% else %}
        AND sp.call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND sp.call_block_time < TIMESTAMP '2025-04-22'
        {% endif %}
)

SELECT
      c.block_slot
    , c.block_month
    , c.block_date
    , c.block_time
    , c.inner_instruction_index
    , c.outer_instruction_index
    , c.outer_executing_account
    , c.is_inner
    , c.tx_id
    , c.tx_signer
    , c.tx_index
    , c.pool_id
    , c.base_token_mint
    , c.quote_token_mint
    , c.base_vault
    , c.quote_vault
    , c.is_buy
    , c.platform_config
    , json_extract_scalar(pc.platform_params, '$.PlatformParams.name') AS platform_name
    , pc.platform_params
    , {{ solana_instruction_key(
          'c.block_slot'
        , 'c.tx_index'
        , 'c.outer_instruction_index'
        , 'c.inner_instruction_index'
      ) }} AS surrogate_key
FROM calls c
LEFT JOIN {{ source('raydium_solana', 'raydium_launchpad_call_create_platform_config') }} pc
    ON c.platform_config = pc.account_platform_config
