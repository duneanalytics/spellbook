{{
  config(
        schema = 'pumpdotfun_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}')
}}

{% set project_start_date = '2024-01-14' %} --grabbed program deployed at time (account created at)

with
    bonding_curves as (
        SELECT
            account_pool as pool_id,
            account_base_mint as token_mint_address,
            account_pool_base_token_account as bonding_curve_vault
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_create_pool') }}
        WHERE 
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
    )

    , swaps as (
        SELECT
            call_block_time as block_time,
            call_block_slot as block_slot,
            base_amount_out as token_amount, -- The amount of token the user receives
            max_quote_amount_in as sol_amount, -- Maximum SOL amount user is willing to spend
            account_base_mint as token_mint_address,
            account_pool as bonding_curve,
            account_user as user,
            call_tx_id as tx_id,
            call_tx_index as tx_index,
            call_outer_instruction_index as outer_instruction_index,
            call_inner_instruction_index as inner_instruction_index,
            call_outer_executing_account as outer_executing_account,
            1 as is_buy -- Buy operation
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }}
        WHERE
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}

        UNION ALL

        SELECT
            call_block_time as block_time,
            call_block_slot as block_slot,
            base_amount_in as token_amount, -- The amount of token the user sells
            min_quote_amount_out as sol_amount, -- Minimum SOL amount user wants to receive
            account_base_mint as token_mint_address,
            account_pool as bonding_curve,
            account_user as user,
            call_tx_id as tx_id,
            call_tx_index as tx_index,
            call_outer_instruction_index as outer_instruction_index,
            call_inner_instruction_index as inner_instruction_index,
            call_outer_executing_account as outer_executing_account,
            0 as is_buy -- Sell operation
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
        WHERE
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
    )

    , trades_base as (
        SELECT
            sp.block_time,
            'pumpdotfun' as project,
            1 as version,
            'solana' as blockchain,
            case 
                when sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' then 'direct'
                else sp.outer_executing_account
            end as trade_source,
            --bought
            case 
                when is_buy = 1 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
            end as token_bought_mint_address,
            case 
                when is_buy = 1 then token_amount
                else sol_amount
            end as token_bought_amount_raw,
            --sold
            case 
                when is_buy = 0 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
            end as token_sold_mint_address,
            case 
                when is_buy = 0 then token_amount
                else sol_amount
            end as token_sold_amount_raw,
            cast(sp.bonding_curve as varchar) as pool_id,
            '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' as project_main_id,
            sp.user as trader_id,
            sp.tx_id,
            sp.outer_instruction_index,
            COALESCE(sp.inner_instruction_index, 0) as inner_instruction_index,
            sp.tx_index,
            sp.block_slot,
            cast(case 
                when is_buy = 0 then bc.pool_id
                else bc.bonding_curve_vault
            end as varchar) as token_sold_vault,
            cast(case 
                when is_buy = 1 then bc.pool_id
                else bc.bonding_curve_vault
            end as varchar) as token_bought_vault
        FROM swaps sp
        LEFT JOIN bonding_curves bc ON bc.token_mint_address = sp.token_mint_address
    )

SELECT
    tb.blockchain,
    tb.project,
    tb.version,
    CAST(date_trunc('month', tb.block_time) AS DATE) as block_month,
    tb.block_time,
    tb.block_slot,
    tb.trade_source,
    tb.token_bought_amount_raw,
    tb.token_sold_amount_raw,
    cast(0.01 as double) as fee_tier,
    tb.token_sold_mint_address,
    tb.token_bought_mint_address,
    tb.token_sold_vault,
    tb.token_bought_vault,
    tb.pool_id as project_program_id,
    tb.project_main_id,
    tb.trader_id,
    tb.tx_id,
    tb.outer_instruction_index,
    tb.inner_instruction_index,
    tb.tx_index
FROM trades_base tb