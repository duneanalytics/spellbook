{{
  config(
        schema = 'pancakeswap_v3_solana',
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


{% set project_start_date = '2025-06-30' %} --grabbed program deployed at time (account created at)

  WITH
    pools as (
        -- come back for fees some other day after we can tie fee account
        SELECT
             ip.account_token_mint_0 as tokenA
            , ip.account_token_vault_0 as tokenAVault
            , ip.account_token_mint_1 as tokenB
            , ip.account_token_vault_1 as tokenBVault
            , ip.account_amm_config as fee_tier
            , ip.account_pool_state as pool_id
            , ip.call_tx_id as init_tx
            , ip.call_block_time as init_time
            , row_number() over (partition by ip.account_pool_state order by ip.call_block_time desc) as recent_init
        FROM {{ source('pancakeswap_solana','amm_v3_call_create_pool') }} ip
    ),

    amm_v3_call_swap_filtered AS (
        SELECT
            account_pool_state
            , call_is_inner
            , call_outer_instruction_index
            , call_inner_instruction_index
            , call_tx_id
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , call_tx_index
        FROM {{ source('pancakeswap_solana', 'amm_v3_call_swap') }}
        WHERE 1=1
        {% if is_incremental() %}
            AND {{ incremental_predicate('call_block_time') }}
        {% else %}
            AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
    )

    , amm_v3_call_swap_v2_filtered AS (
        SELECT
            account_pool_state
            , call_is_inner
            , call_outer_instruction_index
            , call_inner_instruction_index
            , call_tx_id
            , call_block_time
            , call_block_slot
            , call_outer_executing_account
            , call_tx_signer
            , call_tx_index
        FROM {{ source('pancakeswap_solana', 'amm_v3_call_swap_v2') }}
        WHERE 1=1
        {% if is_incremental() %}
            AND {{ incremental_predicate('call_block_time') }}
        {% else %}
            AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
    )

    , sp AS (
        SELECT * FROM amm_v3_call_swap_filtered
        UNION ALL
        SELECT * FROM amm_v3_call_swap_v2_filtered
    )

    , all_swaps as (
        SELECT
            sp.call_block_time as block_time
            , 'pancakeswap' as project
            , 3 as version
            , 'solana' as blockchain
            , call_block_slot as block_slot
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
        FROM sp
        INNER JOIN pools p
            ON sp.account_pool_state = p.pool_id --account 2
            and p.recent_init = 1 --for some reason, some pools get created twice.
        INNER JOIN {{ ref('tokens_solana_transfers') }} tr_1
            ON tr_1.tx_id = sp.call_tx_id
            AND tr_1.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND tr_1.inner_instruction_index = 1)
                OR (sp.call_is_inner = true AND tr_1.inner_instruction_index = sp.call_inner_instruction_index + 1))
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_1.block_time')}}
            {% else %}
            AND tr_1.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
        INNER JOIN {{ ref('tokens_solana_transfers') }} tr_2
            ON tr_2.tx_id = sp.call_tx_id
            AND tr_2.outer_instruction_index = sp.call_outer_instruction_index
            AND ((sp.call_is_inner = false AND tr_2.inner_instruction_index = 2)
                OR (sp.call_is_inner = true AND tr_2.inner_instruction_index = sp.call_inner_instruction_index + 2))
            {% if is_incremental() %}
            AND {{incremental_predicate('tr_2.block_time')}}
            {% else %}
            AND tr_2.block_time >= TIMESTAMP '{{project_start_date}}'
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
    , 'HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM all_swaps tb
