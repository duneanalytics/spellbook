 {{
  config(
        schema = 'raydium_v5',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'surrogate_key'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
        )
}}

{% set project_start_date = '2024-05-16' %} --grabbed program deployed at time (account created at).

WITH all_swaps as (
    SELECT
        sp.call_block_time as block_time
        , sp.call_block_slot as block_slot
        , 'raydium' as project
        , 5 as version
        , 'cpmm' as version_name
        , 'solana' as blockchain
        , case when sp.call_is_inner = False then 'direct'
            else sp.call_outer_executing_account
            end as trade_source
        -- -- token bought is always the second instruction (transfer) in the inner instructions
        , trs_2.amount as token_bought_amount_raw
        , trs_1.amount as token_sold_amount_raw
        , account_poolState as pool_id --p.pool_id
        , sp.call_tx_signer as trader_id
        , sp.call_tx_id as tx_id
        , sp.call_outer_instruction_index as outer_instruction_index
        , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
        , sp.call_tx_index as tx_index
        , sp.account_outputTokenMint as token_bought_mint_address
        , sp.account_inputTokenMint as token_sold_mint_address
        , sp.account_outputVault as token_bought_vault
        , sp.account_inputVault as token_sold_vault
    FROM {{ ref('raydium_v5_solana_stg_decoded_swaps') }} sp
    INNER JOIN {{ source('tokens_solana','transfers') }} trs_1
        ON trs_1.tx_id = sp.call_tx_id
        AND trs_1.block_date = sp.call_block_date
        AND trs_1.block_slot = sp.call_block_slot
        AND trs_1.outer_instruction_index = sp.call_outer_instruction_index
        AND ((sp.call_is_inner = false AND trs_1.inner_instruction_index = 1)
            OR (sp.call_is_inner = true AND trs_1.inner_instruction_index = sp.call_inner_instruction_index + 1))
        AND trs_1.token_mint_address = sp.account_inputTokenMint
        AND trs_1.to_token_account = sp.account_inputVault
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('trs_1.block_time')}}
        {% else -%}
        AND trs_1.block_date >= DATE '{{project_start_date}}'
        {% endif -%}
    INNER JOIN {{ source('tokens_solana','transfers') }} trs_2
        ON trs_2.tx_id = sp.call_tx_id
        AND trs_2.block_date = sp.call_block_date
        AND trs_2.block_slot = sp.call_block_slot
        AND trs_2.outer_instruction_index = sp.call_outer_instruction_index
        AND ((sp.call_is_inner = false AND trs_2.inner_instruction_index = 2)
            OR (sp.call_is_inner = true AND trs_2.inner_instruction_index = sp.call_inner_instruction_index + 2))
        AND trs_2.token_mint_address = sp.account_outputTokenMint
        AND trs_2.from_token_account = sp.account_outputVault
        {% if is_incremental() or true -%}
        AND {{incremental_predicate('trs_2.block_time')}}
        {% else -%}
        AND trs_2.block_date >= DATE '{{project_start_date}}'
        {% endif -%}
    where
		1=1
		{% if is_incremental() or true -%}
		and {{incremental_predicate('sp.call_block_time')}}
		{% else -%}
		and sp.call_block_date >= date '{{project_start_date}}'
		{% endif -%}
)

SELECT
    tb.blockchain
    , tb.project
    , tb.version
    , tb.version_name
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
    , 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , {{ dbt_utils.generate_surrogate_key(['tb.block_slot', 'tb.tx_id', 'tb.tx_index', 'tb.outer_instruction_index', 'tb.inner_instruction_index']) }} as surrogate_key
FROM all_swaps tb