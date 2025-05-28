{{
  config(
    schema = 'stabble_solana',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index', 'block_month']
  )
}}

{% set project_start_date = '2024-04-01' %}

WITH all_swaps AS (
    -- Stable Swap V1
    SELECT
        'stable_swap_v1' AS swap_type,
        call_block_time AS block_time,
        call_block_slot AS block_slot,
        'stabble' AS project,
        1 AS version,
        'solana' AS blockchain,
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END AS trade_source,
        amount_in AS token_sold_amount_raw,
        account_pool AS pool_id,
        call_tx_signer AS trader_id,
        call_tx_id AS tx_id,
        call_outer_instruction_index AS outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS inner_instruction_index,
        call_tx_index AS tx_index,
        CAST(NULL AS VARCHAR) AS token_sold_mint_address,      
        CAST(NULL AS VARCHAR) AS token_bought_mint_address,    
        account_user_token_in,
        account_user_token_out,    
        account_vault_token_in AS token_sold_vault,
        account_vault_token_out AS token_bought_vault
    FROM {{ source('stable_swap_solana', 'stable_swap_call_swap') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('call_block_time')}}
    {% else %}
    WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}

    UNION ALL

    -- Stable Swap V2
    SELECT
        'stable_swap_v2' AS swap_type,
        call_block_time AS block_time,
        call_block_slot AS block_slot,
        'stabble' AS project,
        1 AS version,
        'solana' AS blockchain,
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END,
        amount_in AS token_sold_amount_raw,
        account_pool AS pool_id,
        call_tx_signer AS trader_id,
        call_tx_id AS tx_id,
        call_outer_instruction_index AS outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS inner_instruction_index,
        call_tx_index AS tx_index,
        account_mint_in AS token_sold_mint_address,      
        account_mint_out AS token_bought_mint_address,    
        account_user_token_in,      
        account_user_token_out,    
        account_vault_token_in AS token_sold_vault,
        account_vault_token_out AS token_bought_vault
    FROM {{ source('stable_swap_solana', 'stable_swap_call_swap_v2') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('call_block_time')}}
    {% else %}
    WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}

    UNION ALL

    -- Weighted Swap V1
    SELECT
        'weighted_swap_v1' AS swap_type,
        call_block_time AS block_time,
        call_block_slot AS block_slot,
        'stabble' AS project,
        1 AS version,
        'solana' AS blockchain,
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END AS trade_source,
        amount_in AS token_sold_amount_raw,
        account_pool AS pool_id,
        call_tx_signer AS trader_id,
        call_tx_id AS tx_id,
        call_outer_instruction_index AS outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS inner_instruction_index,
        call_tx_index AS tx_index,
        CAST(NULL AS VARCHAR) AS token_sold_mint_address,      
        CAST(NULL AS VARCHAR) AS token_bought_mint_address,    
        account_user_token_in,      
        account_user_token_out,    
        account_vault_token_in AS token_sold_vault,
        account_vault_token_out AS token_bought_vault
    FROM {{ source('stable_swap_solana', 'weighted_swap_call_swap') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('call_block_time')}}
    {% else %}
    WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}

    UNION ALL

    -- Weighted Swap V2
    SELECT
        'weighted_swap_v2' AS swap_type,
        call_block_time AS block_time,
        call_block_slot AS block_slot,
        'stabble' AS project,
        1 AS version,
        'solana' AS blockchain,
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END AS trade_source,
        amount_in AS token_sold_amount_raw,
        account_pool AS pool_id,
        call_tx_signer AS trader_id,
        call_tx_id AS tx_id,
        call_outer_instruction_index AS outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS inner_instruction_index,
        call_tx_index AS tx_index,
        account_mint_in AS token_sold_mint_address,      
        account_mint_out AS token_bought_mint_address,    
        account_user_token_in,      
        account_user_token_out,    
        account_vault_token_in AS token_sold_vault,
        account_vault_token_out AS token_bought_vault
    FROM {{ source('stable_swap_solana', 'weighted_swap_call_swap_v2') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('call_block_time')}}
    {% else %}
    WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

, transfers AS (
    SELECT
        s.tx_id,
        s.block_slot,
        s.outer_instruction_index,
        s.inner_instruction_index as swap_inner_idx,
        t.inner_instruction_index as transfer_inner_idx,
        t.amount,
        t.token_mint_address
    FROM all_swaps s
    INNER JOIN {{ ref('tokens_solana_transfers') }} t
        ON t.tx_id = s.tx_id 
        AND t.block_slot = s.block_slot
        AND t.outer_instruction_index = s.outer_instruction_index
        AND t.from_token_account = s.token_bought_vault
        AND t.to_token_account = s.account_user_token_out
        AND t.inner_instruction_index IN (
            s.inner_instruction_index + 1,
            s.inner_instruction_index + 2,
            s.inner_instruction_index + 3,
            s.inner_instruction_index + 4
        )
        {% if is_incremental() %}
        AND {{incremental_predicate('t.block_time')}}
        {% else %}
        AND t.block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
)

SELECT
    s.blockchain,
    s.project,
    s.version,
    CAST(DATE_TRUNC('month', s.block_time) AS DATE) AS block_month,
    s.block_time,
    s.block_slot,
    s.trade_source,
    ot.amount AS token_bought_amount_raw, 
    s.token_sold_amount_raw, 
    CAST(NULL AS DOUBLE) AS fee_tier,
    COALESCE(s.token_sold_mint_address, ot.token_mint_address) AS token_sold_mint_address,
    COALESCE(s.token_bought_mint_address, ot.token_mint_address) AS token_bought_mint_address,
    s.token_sold_vault,
    s.token_bought_vault,
    s.pool_id AS project_program_id,
    'vo1tWgqZMjG61Z2T9qUaMYKqZ75CYzMuaZ2LZP1n7HV' AS project_main_id,
    s.trader_id,
    s.tx_id,
    s.outer_instruction_index,
    s.inner_instruction_index,
    s.tx_index
FROM all_swaps s
LEFT JOIN transfers ot
    ON ot.tx_id = s.tx_id
    AND ot.block_slot = s.block_slot
    AND ot.outer_instruction_index = s.outer_instruction_index
    AND ot.swap_inner_idx = s.inner_instruction_index