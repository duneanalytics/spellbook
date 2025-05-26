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
    WHERE call_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}

    UNION ALL

    -- Stable Swap V2
    SELECT
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
    WHERE call_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}

    UNION ALL

    -- Weighted Swap V1
    SELECT
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
    WHERE call_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}

    UNION ALL

    -- Weighted Swap V2
    SELECT
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
    WHERE call_block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}
)

, transfers AS (
    SELECT 
        t.tx_id,
        t.block_slot,
        t.outer_instruction_index,
        t.token_mint_address,
        t.from_token_account,
        t.to_token_account,
        t.amount,
        CASE 
            WHEN t.from_token_account = s.account_user_token_in 
                 AND t.to_token_account = s.token_sold_vault 
            THEN 'user_to_vault'
            WHEN t.from_token_account = s.token_bought_vault 
                 AND t.to_token_account = s.account_user_token_out 
            THEN 'vault_to_user'
        END as transfer_type
    FROM {{ ref('tokens_solana_transfers') }} t
    INNER JOIN all_swaps s 
        ON t.tx_id = s.tx_id 
        AND t.block_slot = s.block_slot
        AND t.outer_instruction_index = s.outer_instruction_index
        AND (
            -- Tokens being sold: User → Pool vault
            (t.from_token_account = s.account_user_token_in 
             AND t.to_token_account = s.token_sold_vault)
            OR
            -- Tokens being bought: Pool vault → User
            (t.from_token_account = s.token_bought_vault 
             AND t.to_token_account = s.account_user_token_out)
        )
    WHERE t.block_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    {% if is_incremental() %}
    AND {{incremental_predicate('t.block_time')}}
    {% endif %}
)

, swaps_with_transfers AS (
    SELECT
        s.*,
        -- Get token bought amount (vault -> user transfers)
        t_bought.amount AS token_bought_amount_raw,
        -- Fill missing mint addresses from transfers for V1 tables
        COALESCE(s.token_sold_mint_address, t_sold.token_mint_address) AS final_token_sold_mint_address,
        COALESCE(s.token_bought_mint_address, t_bought.token_mint_address) AS final_token_bought_mint_address
    FROM all_swaps s
    
    -- Join for tokens bought (from vault to user)
    LEFT JOIN transfers t_bought
        ON t_bought.tx_id = s.tx_id
        AND t_bought.block_slot = s.block_slot
        AND t_bought.outer_instruction_index = s.outer_instruction_index
        AND t_bought.from_token_account = s.token_bought_vault
        AND t_bought.to_token_account = s.account_user_token_out
        
    -- Join for tokens sold (from user to vault) - to get mint address for V1 tables
    LEFT JOIN transfers t_sold
        ON t_sold.tx_id = s.tx_id
        AND t_sold.block_slot = s.block_slot
        AND t_sold.outer_instruction_index = s.outer_instruction_index
        AND t_sold.from_token_account = s.account_user_token_in
        AND t_sold.to_token_account = s.token_sold_vault
)

SELECT
    tb.blockchain,
    tb.project,
    tb.version,
    CAST(DATE_TRUNC('month', tb.block_time) AS DATE) AS block_month,
    tb.block_time,
    tb.block_slot,
    tb.trade_source,
    tb.token_bought_amount_raw,
    tb.token_sold_amount_raw,
    CAST(NULL AS DOUBLE) AS fee_tier,
    tb.final_token_sold_mint_address AS token_sold_mint_address,
    tb.final_token_bought_mint_address AS token_bought_mint_address,
    tb.token_sold_vault,
    tb.token_bought_vault,
    tb.pool_id AS project_program_id,
    'vo1tWgqZMjG61Z2T9qUaMYKqZ75CYzMuaZ2LZP1n7HV' AS project_main_id,
    tb.trader_id,
    tb.tx_id,
    tb.outer_instruction_index,
    tb.inner_instruction_index,
    tb.tx_index
FROM swaps_with_transfers tb
