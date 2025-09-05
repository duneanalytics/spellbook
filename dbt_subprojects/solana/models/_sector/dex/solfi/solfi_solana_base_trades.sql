{{
  config(
    schema = 'solfi_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-09-01' %}

-- Base swaps from solfi_call_swap table
WITH solfi_swaps AS (
    SELECT
        call_block_time as block_time
        , call_block_slot as block_slot
        , call_tx_id as tx_id
        , call_tx_index as tx_index
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as inner_instruction_index
        , call_tx_signer as trader_id
        , call_outer_executing_account as outer_executing_account
        , call_inner_executing_account as inner_executing_account
        , call_is_inner as is_inner_swap
        , account_pair
        , account_poolTokenAccountA
        , account_poolTokenAccountB
        , account_user
        , account_userTokenAccountA
        , account_userTokenAccountB
        , direction
    FROM {{ source('solfi_solana', 'solfi_call_swap') }}
    WHERE 1=1
    {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
    {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)
-- Get both input and output transfers for each swap
, swap_transfers AS (
    SELECT
        s.*,
        -- Input transfer (user to pool) - gives us SOLD token info
        t_input.amount as token_sold_amount_raw,
        t_input.token_mint_address as token_sold_mint_address,
        t_input.to_token_account as token_sold_vault,  -- pool account receiving
        
        -- Output transfer (pool to user) - gives us BOUGHT token info  
        t_output.amount as token_bought_amount_raw,
        t_output.token_mint_address as token_bought_mint_address,
        t_output.from_token_account as token_bought_vault  -- pool account sending
        
    FROM solfi_swaps s
    
    -- Join for INPUT transfer (what user sells to pool)
    INNER JOIN {{ source('tokens_solana','transfers') }} t_input
        ON t_input.tx_id = s.tx_id
        AND t_input.block_slot = s.block_slot
        AND t_input.block_time = s.block_time  -- Add block_time
        AND t_input.outer_instruction_index = s.outer_instruction_index
        AND (
            -- For non-inner swaps: input transfer is at instruction index 1
            (s.is_inner_swap = false AND t_input.inner_instruction_index = 1)
            OR
            -- For inner swaps: input transfer is at swap instruction + 1
            (s.is_inner_swap = true AND t_input.inner_instruction_index = s.inner_instruction_index + 1)
        )
        -- Input transfer: from user account to pool account
        AND t_input.from_token_account IN (s.account_userTokenAccountA, s.account_userTokenAccountB)
        AND t_input.to_token_account IN (s.account_poolTokenAccountA, s.account_poolTokenAccountB)
        {% if is_incremental() %}
        AND {{ incremental_predicate('t_input.block_time') }}
        {% else %}
        AND t_input.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
    
    -- Join for OUTPUT transfer (what user receives from pool)
    INNER JOIN {{ source('tokens_solana','transfers') }} t_output
        ON t_output.tx_id = s.tx_id
        AND t_output.block_slot = s.block_slot
        AND t_output.outer_instruction_index = s.outer_instruction_index
        AND (
            -- For non-inner swaps: output transfer is at instruction index 2
            (s.is_inner_swap = false AND t_output.inner_instruction_index = 2)
            OR
            -- For inner swaps: output transfer is at swap instruction + 2
            (s.is_inner_swap = true AND t_output.inner_instruction_index = s.inner_instruction_index + 2)
        )
        -- Output transfer: from pool account to user account
        AND t_output.from_token_account IN (s.account_poolTokenAccountA, s.account_poolTokenAccountB)
        AND t_output.to_token_account IN (s.account_userTokenAccountA, s.account_userTokenAccountB)
        {% if is_incremental() %}
        AND {{ incremental_predicate('t_output.block_time') }}
        {% else %}
        AND t_output.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
)

-- Build the base trades
, trades_base AS (
    SELECT
        st.block_time
        , 'solfi' as project
        , 1 as version
        , 'solana' as blockchain
        , CASE 
            WHEN st.is_inner_swap = false THEN 'direct'
            ELSE st.outer_executing_account
          END as trade_source
        , st.token_bought_mint_address
        , st.token_bought_amount_raw
        , st.token_sold_mint_address
        , st.token_sold_amount_raw
        
        , CAST(NULL AS DOUBLE) as fee_tier
        , st.account_pair as pool_id
        , 'SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe' as project_main_id
        , st.trader_id
        , st.tx_id
        , st.outer_instruction_index
        , st.inner_instruction_index
        , st.tx_index
        , st.block_slot
        
        -- Token vaults come directly from transfers
        , st.token_bought_vault
        , st.token_sold_vault
            
    FROM swap_transfers st
)

SELECT
    tb.blockchain
    , tb.project
    , tb.version
    , CAST(DATE_TRUNC('month', tb.block_time) AS DATE) as block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , tb.fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , tb.project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , COALESCE(tb.inner_instruction_index, 0) as inner_instruction_index
    , tb.tx_index
    , {{ dbt_utils.generate_surrogate_key(['tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']) }} as surrogate_key
FROM trades_base tb