 {{
  config(

        schema = 'lifinity_v1',
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

{% set project_start_date = '2022-01-26' %} --grabbed program deployed at time (account created at)

WITH
    all_swaps as (
        SELECT
            sp.call_block_time as block_time
            , sp.call_block_slot as block_slot
            , 'lifinity' as project
            , 1 as version
            , 'solana' as blockchain
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            -- token bought is always the second instruction (transfer) in the inner instructions
            , tr_2.amount as token_bought_amount_raw
            , tr_1.amount as token_sold_amount_raw
            , sp.account_amm as pool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , COALESCE(tr_2.token_mint_address, cast(null as varchar)) as token_bought_mint_address
            , COALESCE(tr_1.token_mint_address, cast(null as varchar)) as token_sold_mint_address
            , tr_2.from_token_account as token_bought_vault
            , tr_1.to_token_account as token_sold_vault
            --swap out can be either 2nd or 3rd transfer, we need to filter for the first transfer out.
            , tr_2.inner_instruction_index as transfer_out_index
            , row_number() over (partition by sp.call_tx_id, sp.call_outer_instruction_index, sp.call_inner_instruction_index
                                order by COALESCE(tr_2.inner_instruction_index, 0) asc) as first_transfer_out
        FROM {{ source('lifinity_amm_solana', 'lifinity_amm_call_swap') }} sp
        INNER JOIN {{ source('tokens_solana','transfers') }} tr_1
            ON tr_1.tx_id = sp.call_tx_id AND tr_1.action = 'transfer'
            AND tr_1.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND tr_1.inner_instruction_index = 1)
                OR (sp.call_is_inner = true AND tr_1.inner_instruction_index = sp.call_inner_instruction_index + 1))
            AND tr_1.token_version = 'spl_token'
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_1.block_time')}}
            {% else %}
            AND tr_1.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        --swap out can be either 2nd or 3rd transfer.
        INNER JOIN {{ source('tokens_solana','transfers') }} tr_2
            ON tr_2.tx_id = sp.call_tx_id AND tr_2.action = 'transfer'
            AND tr_2.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND (tr_2.inner_instruction_index = 2 OR tr_2.inner_instruction_index = 3))
                OR (sp.call_is_inner = true AND (tr_2.inner_instruction_index = sp.call_inner_instruction_index + 2 OR tr_2.inner_instruction_index = sp.call_inner_instruction_index + 3))
                )
            AND tr_2.token_version = 'spl_token'
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_2.block_time')}}
            {% else %}
            AND tr_2.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        WHERE 1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('sp.call_block_time')}}
        {% else %}
        AND sp.call_block_time >= TIMESTAMP '{{project_start_date}}'
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
    , 'EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb
WHERE first_transfer_out = 1
