 {{
  config(
        schema = 'meteora_v1_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month']
        )
}}

{% set project_start_date = '2021-03-21' %} --grabbed program deployed at time (account created at).

WITH
    all_swaps as (
        SELECT 
            sp.call_block_time as block_time
            , sp.call_block_slot as block_slot
            , 'meteora' as project
            , 1 as version
            , 'solana' as blockchain
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            -- -- token bought is always the second instruction (transfer) in the inner instructions
            , trs_2.amount as token_bought_amount_raw
            , trs_1.amount as token_sold_amount_raw
            , sp.account_pool as pool_id --p.pool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , COALESCE(trs_2.token_mint_address, cast(null as varchar)) as token_bought_mint_address
            , COALESCE(trs_1.token_mint_address, cast(null as varchar)) as token_sold_mint_address
            , trs_2.from_token_account as token_bought_vault
            , trs_1.to_token_account as token_sold_vault
        FROM (
            SELECT 
                sp.*
                , dp.call_inner_instruction_index as deposit_index
                , row_number() over (partition by sp.call_tx_id, sp.call_outer_instruction_index, sp.call_inner_instruction_index order by dp.call_inner_instruction_index asc) as first_deposit
            FROM 
                {{ source('meteora_pools_solana', 'amm_call_swap') }} sp
            LEFT JOIN 
                {{ source('meteora_vault_solana', 'vault_call_deposit') }} dp ON sp.call_tx_id = dp.call_tx_id 
                AND sp.call_block_slot = dp.call_block_slot
                AND sp.call_outer_instruction_index = dp.call_outer_instruction_index 
                and COALESCE(sp.call_inner_instruction_index, 0) < dp.call_inner_instruction_index
                {% if is_incremental() %}
                AND {{incremental_predicate('dp.call_block_time')}}
                {% else %}
                AND dp.call_block_time >= TIMESTAMP '{{project_start_date}}'
                {% endif %}
            WHERE 
                1=1 
                {% if is_incremental() %}
                AND {{incremental_predicate('sp.call_block_time')}}
                {% else %}
                AND sp.call_block_time >= TIMESTAMP '{{project_start_date}}'
                {% endif %}
        ) sp
        INNER JOIN 
            {{ source('tokens_solana','transfers') }} trs_1 
            ON trs_1.tx_id = sp.call_tx_id 
            AND trs_1.block_date = sp.call_block_date
            AND trs_1.block_time = sp.call_block_time
            AND trs_1.outer_instruction_index = sp.call_outer_instruction_index 
            AND trs_1.inner_instruction_index = sp.deposit_index + 1
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
            AND trs_2.inner_instruction_index = sp.deposit_index + 4
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_2.block_time')}}
            {% else %}
            AND trs_2.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        WHERE
            1=1
            and first_deposit = 1 --keep only the first deposit after swap invoke
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
    , 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb