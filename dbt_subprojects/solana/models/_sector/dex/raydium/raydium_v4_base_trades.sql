 {{
  config(
        schema = 'raydium_v4',
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

{% set project_start_date = '2021-03-21' %} --grabbed program deployed at time (account created at).

  WITH
    --we aren't tracking using pool inits because there are a hundred or so pools from 2021 that did not use a consistent pattern.
    all_swaps as (
        SELECT
            sp.call_block_time as block_time
            , sp.call_block_slot as block_slot
            , 'raydium' as project
            , 4 as version
            , 'solana' as blockchain
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            -- -- token bought is always the second instruction (transfer) in the inner instructions
            , trs_2.amount as token_bought_amount_raw
            , trs_1.amount as token_sold_amount_raw
            , account_amm as pool_id --p.pool_id
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
            SELECT account_serumMarket, account_amm, call_is_inner, call_outer_instruction_index, call_inner_instruction_index, call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer, call_tx_index
            FROM {{ source('raydium_amm_solana', 'raydium_amm_call_swapBaseOut') }}
            UNION ALL
            SELECT account_serumMarket, account_amm, call_is_inner, call_outer_instruction_index, call_inner_instruction_index, call_tx_id, call_block_time, call_block_slot, call_outer_executing_account, call_tx_signer, call_tx_index
            FROM {{ source('raydium_amm_solana', 'raydium_amm_call_swapBaseIn') }}
        ) sp
        INNER JOIN {{ source('tokens_solana','transfers') }} trs_1
            ON trs_1.tx_id = sp.call_tx_id
            AND trs_1.block_time = sp.call_block_time
            AND trs_1.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND (trs_1.inner_instruction_index = 1 OR trs_1.inner_instruction_index = 2))
                OR (sp.call_is_inner = true AND (trs_1.inner_instruction_index = sp.call_inner_instruction_index + 1 OR trs_1.inner_instruction_index = sp.call_inner_instruction_index + 2))
                )
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_1.block_time')}}
            {% else %}
            AND trs_1.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ source('tokens_solana','transfers') }} trs_2
            ON trs_2.tx_id = sp.call_tx_id
            AND trs_2.block_time = sp.call_block_time
            AND trs_2.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND (trs_2.inner_instruction_index = 2 OR trs_2.inner_instruction_index = 3))
                OR (sp.call_is_inner = true AND (trs_2.inner_instruction_index = sp.call_inner_instruction_index + 2 OR trs_2.inner_instruction_index = sp.call_inner_instruction_index + 3))
                )
            {% if is_incremental() %}
            AND {{incremental_predicate('trs_2.block_time')}}
            {% else %}
            AND trs_2.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        LEFT JOIN {{ ref('solana_utils_token_accounts') }} tk_2 ON tk_2.address = trs_2.from_token_account
        WHERE 1=1
        and trs_1.token_mint_address != trs_2.token_mint_address --gets rid of dupes from the OR statement in transfer joins
        and tk_2.token_balance_owner = '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1' --raydium pool v4 authority. makes sure we don't accidently catch some fee transfer or something after the swap. should add for lifinity too later.
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
    , '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb
