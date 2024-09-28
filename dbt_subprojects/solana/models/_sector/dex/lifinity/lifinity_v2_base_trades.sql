 {{
  config(

        schema = 'lifinity_v2',
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

{% set project_start_date = '2022-09-13' %} --grabbed program deployed at time (account created at)

WITH
    pools as (
        -- we can get fees after they give us the right IDL for initializing the pool and updating configs
        -- https://solscan.io/tx/DNXYzbhFnY9PwT4iwXNMpQq42kafcPaxSSgxsZ6XFLACvVNfpEfbJHG6VjPKevnH3aT4nwqPy4WFmQu4Y4NrY3e
        SELECT
             mintA.token_mint_address as tokenA
            , ip.account_arguments[4] as tokenAVault
            , mintB.token_mint_address as tokenB
            , ip.account_arguments[5] as tokenBVault
            , ip.account_arguments[6] as fee_account
            , ip.account_arguments[2] as pool_id
            , ip.account_arguments[3] as pool_mint_id
            , ip.tx_id as init_tx
        FROM (
            SELECT
            *
            FROM {{ source('solana','instruction_calls') }}
            WHERE cardinality(account_arguments) >= 5 --filter out broken cases/inits for now
            and bytearray_substring(data,1,8) = 0xafaf6d1f0d989bed
            and executing_account = '2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c'
            and tx_success
            and block_time > TIMESTAMP '2022-01-26'
        ) ip
        INNER JOIN {{ ref('solana_utils_token_accounts') }} mintA ON mintA.address = ip.account_arguments[4]
            AND mintA.account_type = 'fungible'
        INNER JOIN {{ ref('solana_utils_token_accounts') }} mintB ON mintB.address = ip.account_arguments[5]
            AND mintB.account_type = 'fungible'
    )

    , all_swaps as (
        SELECT
            sp.call_block_time as block_time
            , sp.call_block_slot as block_slot
            , 'lifinity' as project
            , 2 as version
            , 'solana' as blockchain
            , case when sp.call_is_inner = False then 'direct'
                else sp.call_outer_executing_account
                end as trade_source
            -- token bought is always the second instruction (transfer) in the inner instructions
            , tr_2.amount as token_bought_amount_raw
            , tr_1.amount as token_sold_amount_raw
            , p.pool_id
            , sp.call_tx_signer as trader_id
            , sp.call_tx_id as tx_id
            , sp.call_outer_instruction_index as outer_instruction_index
            , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
            , sp.call_tx_index as tx_index
            , case when tr_1.token_mint_address = p.tokenA then p.tokenB
                else p.tokenA
                end as token_bought_mint_address
            , case when tr_1.token_mint_address = p.tokenA then p.tokenA
                else p.tokenB
                end as token_sold_mint_address
            , case when tr_1.token_mint_address = p.tokenA then p.tokenBVault
                else p.tokenAVault
                end as token_bought_vault
            , case when tr_1.token_mint_address = p.tokenA then p.tokenAVault
                else p.tokenBVault
                end as token_sold_vault
            --swap out can be either 2nd or 3rd transfer, we need to filter for the first transfer out.
            , tr_2.inner_instruction_index as transfer_out_index
            , row_number() over (partition by sp.call_tx_id, sp.call_outer_instruction_index, sp.call_inner_instruction_index
                                order by COALESCE(tr_2.inner_instruction_index, 0) asc) as first_transfer_out
        FROM {{ source('lifinity_amm_v2_solana', 'lifinity_amm_v2_call_swap') }} sp
        INNER JOIN pools p
            ON sp.account_amm = p.pool_id --account 2
        INNER JOIN {{ ref('tokens_solana_transfers') }} tr_1
            ON tr_1.tx_id = sp.call_tx_id
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
        INNER JOIN {{ ref('tokens_solana_transfers') }} tr_2
            ON tr_2.tx_id = sp.call_tx_id
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
    , '2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb
WHERE first_transfer_out = 1
