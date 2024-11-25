{{
  config(
        schema = 'sanctum_router_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2023-02-03' %} 

WITH swap_via_stake AS (
    SELECT 
        call_block_time as block_time
        , call_block_slot as block_slot
        , call_tx_signer as trader_id
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) as inner_instruction_index
        , call_tx_index as tx_index
        , account_srcTokenFrom as token_sold_vault
        , account_destTokenTo as token_bought_vault
        , account_srcTokenMint as token_sold_mint_address
        , account_destTokenMint as token_bought_mint_address
        , bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data, 2, 8))) AS token_sold_amount_raw
        , CASE 
            WHEN call_outer_executing_account = 'stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq' THEN 'direct'
            ELSE call_outer_executing_account 
          END as trade_source
        , 'mintTo' as amount_type
    FROM {{ source('sanctum_router_solana', 'stakedex_call_SwapViaStake') }}
    WHERE 1=1
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% else %}
    AND call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
),

prefund_swap_via_stake AS (
    SELECT 
        call_block_time as block_time
        , call_block_slot as block_slot
        , call_tx_signer as trader_id
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) as inner_instruction_index
        , call_tx_index as tx_index
        , account_srcTokenFrom as token_sold_vault
        , account_destTokenTo as token_bought_vault
        , account_srcTokenMint as token_sold_mint_address
        , account_destTokenMint as token_bought_mint_address
        , bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data, 2, 8))) AS token_sold_amount_raw
        , CASE 
            WHEN call_outer_executing_account = 'stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq' THEN 'direct'
            ELSE call_outer_executing_account 
          END as trade_source
        , 'transferChecked' as amount_type
    FROM {{ source('sanctum_router_solana', 'stakedex_call_PrefundSwapViaStake') }}
    WHERE 1=1
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% else %}
    AND call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
),

stake_wrapped_sol AS (
    SELECT 
        call_block_time as block_time
        , call_block_slot as block_slot
        , call_tx_signer as trader_id
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , COALESCE(call_inner_instruction_index, 0) as inner_instruction_index
        , call_tx_index as tx_index
        , account_wsolFrom as token_sold_vault
        , account_destTokenTo as token_bought_vault
        , account_wsolMint as token_sold_mint_address
        , account_destTokenMint as token_bought_mint_address
        , bytearray_to_bigint(bytearray_reverse(bytearray_substring(call_data, 2, 8))) AS token_sold_amount_raw
        , CASE 
            WHEN call_outer_executing_account = 'stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq' THEN 'direct'
            ELSE call_outer_executing_account 
          END as trade_source
        , 'transferChecked' as amount_type
    FROM {{ source('sanctum_router_solana', 'stakedex_call_StakeWrappedSol') }}
    WHERE 1=1
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% else %}
    AND call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
),

withdraw_deposit AS (
    SELECT 
        w.call_block_time as block_time
        , w.call_block_slot as block_slot
        , w.call_tx_signer as trader_id
        , w.call_tx_id as tx_id
        , w.call_outer_instruction_index as outer_instruction_index
        , COALESCE(w.call_inner_instruction_index, 0) as inner_instruction_index
        , w.call_tx_index as tx_index
        , w.account_srcTokenFrom as token_sold_vault
        , d.account_destTokenTo as token_bought_vault
        , w.account_srcTokenMint as token_sold_mint_address
        , d.account_destTokenMint as token_bought_mint_address
        , bytearray_to_bigint(bytearray_reverse(bytearray_substring(w.call_data, 2, 8))) AS token_sold_amount_raw
        , CASE 
            WHEN w.call_outer_executing_account = 'stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq' THEN 'direct'
            ELSE w.call_outer_executing_account 
          END as trade_source
        , 'transferChecked' as amount_type
    FROM {{ source('sanctum_router_solana', 'stakedex_call_PrefundWithdrawStake') }} w
    INNER JOIN {{ source('sanctum_router_solana', 'stakedex_call_DepositStake') }} d
        ON w.call_tx_id = d.call_tx_id 
        AND w.call_outer_instruction_index = d.call_outer_instruction_index
        AND w.call_inner_instruction_index < d.call_inner_instruction_index
    WHERE 1=1
    {% if is_incremental() %}
    AND {{incremental_predicate('w.call_block_time')}}
    {% else %}
    AND w.call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
),

all_trades AS (
    SELECT * FROM swap_via_stake
    UNION ALL 
    SELECT * FROM prefund_swap_via_stake
    UNION ALL
    SELECT * FROM stake_wrapped_sol
    UNION ALL
    SELECT * FROM withdraw_deposit
),

token_amounts AS (
    SELECT 
        ic.tx_id,
        ic.outer_instruction_index,
        ic.inner_instruction_index,
        bytearray_to_bigint(bytearray_reverse(bytearray_substring(ic.data, 2, 8))) AS amount_bought,
        ROW_NUMBER() OVER (
            PARTITION BY ic.tx_id, ic.outer_instruction_index
            ORDER BY 
                CASE 
                    WHEN b.amount_type = 'mintTo' THEN ic.inner_instruction_index
                    ELSE -ic.inner_instruction_index
                END
        ) as rn
    FROM all_trades b
    INNER JOIN {{ source('solana','instruction_calls') }} ic  
        ON ic.tx_id = b.tx_id 
        AND ic.outer_instruction_index = b.outer_instruction_index
        AND ic.block_slot = b.block_slot
    WHERE 1=1
        AND ic.executing_account = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        AND (
            (b.amount_type = 'mintTo' AND bytearray_substring(ic.data, 1, 1) = 0x07 AND ELEMENT_AT(ic.account_arguments, 1) = b.token_bought_mint_address)
            OR 
            (b.amount_type = 'transferChecked' AND bytearray_substring(ic.data, 1, 1) = 0x0c AND ELEMENT_AT(ic.account_arguments, 2) = b.token_bought_mint_address AND ELEMENT_AT(ic.account_arguments, 3) = b.token_bought_vault)
        )
        {% if is_incremental() %}
        AND {{incremental_predicate('ic.block_time')}}
        {% else %}
        AND ic.block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
)

SELECT
    'solana' as blockchain
    , 'sanctum_router' as project
    , 1 as version
    , CAST(date_trunc('month', b.block_time) AS DATE) as block_month
    , b.block_time
    , b.block_slot
    , b.trade_source
    , t.amount_bought as token_bought_amount_raw
    , b.token_sold_amount_raw
    , CAST(NULL as double) as fee_tier
    , b.token_bought_mint_address
    , b.token_sold_mint_address
    , b.token_bought_vault
    , b.token_sold_vault
    , CAST(NULL as varchar) as project_program_id
    , 'stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq' as project_main_id
    , b.trader_id
    , b.tx_id
    , b.outer_instruction_index
    , b.inner_instruction_index
    , b.tx_index
FROM all_trades b
INNER JOIN token_amounts t
    ON b.tx_id = t.tx_id 
    AND b.outer_instruction_index = t.outer_instruction_index
    AND t.rn = 1
