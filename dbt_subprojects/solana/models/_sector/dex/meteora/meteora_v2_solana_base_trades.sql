 {{
  config(
        schema = 'meteora_v2_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month']
        )
}}

{% set project_start_date = '2023-11-01' %} --grabbed program deployed at time (account created at).

WITH
    all_swaps as (
        SELECT 
            sp.call_block_time as block_time
            , sp.call_block_slot as block_slot
            , 'meteora' as project
            , 2 as version
            , 'solana' as blockchain
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            -- -- token bought is always the second instruction (transfer) in the inner instructions
            , trs_2.amount as token_bought_amount_raw
            , trs_1.amount as token_sold_amount_raw
            , sp.account_lbPair as pool_id --p.pool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , COALESCE(trs_2.token_mint_address, cast(null as varchar)) as token_bought_mint_address
            , COALESCE(trs_1.token_mint_address, cast(null as varchar)) as token_sold_mint_address
            , trs_2.from_token_account as token_bought_vault
            , trs_1.to_token_account as token_sold_vault
        FROM 
            {{ source('meteora_solana','lb_clmm_call_swap') }}  sp
        INNER JOIN 
            {{ source('tokens_solana','transfers') }} trs_1 
            ON trs_1.tx_id = sp.call_tx_id 
            AND trs_1.block_date = sp.call_block_date
            AND trs_1.block_time = sp.call_block_time
            AND trs_1.outer_instruction_index = sp.call_outer_instruction_index 
            AND trs_1.inner_instruction_index = COALESCE(sp.call_inner_instruction_index,0) + 1
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_1.block_time')}}
            {% else %}
            AND trs_1.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN 
            {{ source('tokens_solana','transfers') }} trs_2 
            ON trs_2.tx_id = sp.call_tx_id 
            AND trs_2.block_date = sp.call_block_date
            AND trs_2.block_time = sp.call_block_time
            AND trs_2.outer_instruction_index = sp.call_outer_instruction_index 
            AND trs_2.inner_instruction_index = COALESCE(sp.call_inner_instruction_index,0) + 2
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_2.block_time')}}
            {% else %}
            AND trs_2.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        WHERE 
            1=1
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
    , 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb