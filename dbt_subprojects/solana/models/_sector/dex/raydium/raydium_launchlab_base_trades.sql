{{
  config(
        schema = 'raydium_launchlab',
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

{% set project_start_date = '2025-03-17' %}

WITH
    -- Identify Launchlab program calls for SellExactIn and BuyExactIn instructions
    launchlab_calls as (
        SELECT
            block_time as call_block_time,
            block_slot as call_block_slot,
            is_inner as call_is_inner,
            outer_instruction_index as call_outer_instruction_index,
            inner_instruction_index as call_inner_instruction_index,
            tx_id as call_tx_id,
            outer_executing_account as call_outer_executing_account,
            tx_signer as call_tx_signer,
            tx_index as call_tx_index,
            log_messages
        FROM
            solana.instruction_calls
        WHERE
            executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
            AND (
                strpos(array_join(log_messages, ' '), 'Instruction: SellExactIn') > 0
                OR strpos(array_join(log_messages, ' '), 'Instruction: BuyExactIn') > 0
            )
            AND block_time >= TIMESTAMP '{{project_start_date}}'
            {% if is_incremental() %}
            AND {{incremental_predicate('block_time')}}
            {% endif %}
    ),
    
    all_swaps as (
        SELECT
            lc.call_block_time as block_time
            , lc.call_block_slot as block_slot
            , 'raydium' as project
            , 'launchlab' as version
            , 'solana' as blockchain
            , case when lc.call_is_inner = False then 'direct'
                else lc.call_outer_executing_account
                end as trade_source
            -- token bought is always the second instruction (transfer) in the inner instructions
            , trs_2.amount as token_bought_amount_raw
            , trs_1.amount as token_sold_amount_raw
            , 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj' as pool_id
            , lc.call_tx_signer as trader_id
            , lc.call_tx_id as tx_id
            , lc.call_outer_instruction_index as outer_instruction_index
            , COALESCE(lc.call_inner_instruction_index, 0) as inner_instruction_index
            , lc.call_tx_index as tx_index
            , COALESCE(trs_2.token_mint_address, cast(null as varchar)) as token_bought_mint_address
            , COALESCE(trs_1.token_mint_address, cast(null as varchar)) as token_sold_mint_address
            , trs_2.from_token_account as token_bought_vault
            , trs_1.to_token_account as token_sold_vault
        FROM 
            launchlab_calls lc
        INNER JOIN {{ ref('tokens_solana_transfers') }} trs_1
            ON trs_1.tx_id = lc.call_tx_id
            AND trs_1.block_time = lc.call_block_time
            AND trs_1.outer_instruction_index = lc.call_outer_instruction_index
            AND ((lc.call_is_inner = false AND (trs_1.inner_instruction_index = 1 OR trs_1.inner_instruction_index = 2))
                OR (lc.call_is_inner = true AND (trs_1.inner_instruction_index = lc.call_inner_instruction_index + 1 OR trs_1.inner_instruction_index = lc.call_inner_instruction_index + 2))
                )
            AND trs_1.token_version = 'spl_token'
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_1.block_time')}}
            {% else %}
            AND trs_1.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ ref('tokens_solana_transfers') }} trs_2
            ON trs_2.tx_id = lc.call_tx_id
            AND trs_2.block_time = lc.call_block_time
            AND trs_2.outer_instruction_index = lc.call_outer_instruction_index
            AND ((lc.call_is_inner = false AND (trs_2.inner_instruction_index = 2 OR trs_2.inner_instruction_index = 3))
                OR (lc.call_is_inner = true AND (trs_2.inner_instruction_index = lc.call_inner_instruction_index + 2 OR trs_2.inner_instruction_index = lc.call_inner_instruction_index + 3))
                )
            AND trs_2.token_version = 'spl_token'
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_2.block_time')}}
            {% else %}
            AND trs_2.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        WHERE 1=1
        AND trs_1.token_mint_address != trs_2.token_mint_address --gets rid of dupes from the OR statement in transfer joins
        {% if is_incremental() %}
        AND {{incremental_predicate('lc.call_block_time')}}
        {% else %}
        AND lc.call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
    )

SELECT
    tb.blockchain
    , tb.project
    , tb.version
    , CAST(date_trunc('month', tb.block_time) AS DATE) as block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , cast(null as double) as fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb