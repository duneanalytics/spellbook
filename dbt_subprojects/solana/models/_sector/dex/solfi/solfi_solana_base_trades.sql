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

{% set project_start_date = '2024-10-29' %}

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
        , amountIn
        , direction
    FROM {{ source('solfi_solana', 'solfi_call_swap') }}
    WHERE 1=1
    {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
    {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)

-- Get pool mint addresses efficiently
, pool_mint_addresses AS (
    SELECT DISTINCT 
        COALESCE(from_token_account, to_token_account) as token_account,
        token_mint_address,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(from_token_account, to_token_account) ORDER BY token_mint_address) as rn
    FROM {{ source('tokens_solana','transfers') }}
    WHERE token_mint_address IS NOT NULL
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)

-- Join with token transfers to get the output amounts and mint addresses
, swaps_with_transfers AS (
    SELECT
        s.*,
        s.amountIn as token_sold_amount_raw,
        t.amount as token_bought_amount_raw,
        t.token_mint_address as token_bought_mint_address,
        -- Get mint addresses from pool data
        tm_a.token_mint_address as mint_a,
        tm_b.token_mint_address as mint_b
    FROM solfi_swaps s
    -- Join to get mint address for pool token account A
    LEFT JOIN pool_mint_addresses tm_a 
        ON tm_a.token_account = s.account_poolTokenAccountA 
        AND tm_a.rn = 1
    -- Join to get mint address for pool token account B  
    LEFT JOIN pool_mint_addresses tm_b 
        ON tm_b.token_account = s.account_poolTokenAccountB 
        AND tm_b.rn = 1
    -- Main transfer join for the output transfer
    INNER JOIN {{ source('tokens_solana','transfers') }} t
        ON t.tx_id = s.tx_id
        AND t.block_slot = s.block_slot
        AND t.outer_instruction_index = s.outer_instruction_index
        AND (
            -- For non-inner swaps: output transfer is at instruction index 2
            (s.is_inner_swap = false AND t.inner_instruction_index = 2)
            OR
            -- For inner swaps: output transfer is at swap instruction + 2
            (s.is_inner_swap = true AND t.inner_instruction_index = s.inner_instruction_index + 2)
        )
        AND (
            -- Verifing here that this is the correct output transfer based on direction
            CASE 
                WHEN s.direction = 0 THEN t.from_token_account = s.account_poolTokenAccountB 
                                      AND t.to_token_account = s.account_userTokenAccountB
                ELSE t.from_token_account = s.account_poolTokenAccountA
                     AND t.to_token_account = s.account_userTokenAccountA
            END
        )
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.block_time') }}
        {% else %}
        AND t.block_time >= TIMESTAMP '{{ project_start_date }}'
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
        
        -- Token bought (what user receives) - get from transfer mint address
        , st.token_bought_mint_address
        , st.token_bought_amount_raw
        
        -- Token sold (what user gives) - determine from direction and pool mints
        , CASE 
            WHEN st.direction = 0 THEN st.mint_a  -- direction 0: user sells tokenA
            ELSE st.mint_b                        -- direction 1: user sells tokenB
          END as token_sold_mint_address
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
        
        -- Token vaults (pool accounts)
        , CASE
            WHEN st.direction = 0 THEN st.account_poolTokenAccountB  -- direction 0: bought from poolB
            ELSE st.account_poolTokenAccountA                       -- direction 1: bought from poolA
          END as token_bought_vault
        , CASE
            WHEN st.direction = 0 THEN st.account_poolTokenAccountA  -- direction 0: sold to poolA
            ELSE st.account_poolTokenAccountB                       -- direction 1: sold to poolB
          END as token_sold_vault
            
    FROM swaps_with_transfers st
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