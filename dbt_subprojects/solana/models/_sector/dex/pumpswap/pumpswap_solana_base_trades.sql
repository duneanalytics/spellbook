{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['tx_id', 'outer_instruction_index', 'swap_inner_index', 'tx_index','block_month']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-03-14' %}

WITH pools AS (
    SELECT
        pool
        , baseMint
        , quoteMint
        , baseMintDecimals
        , quoteMintDecimals
    FROM {{ ref('pumpswap_solana_pools') }}
)

, swaps_base AS (
    -- Buy operations
    SELECT
        call_block_time as block_time
        , call_block_slot as block_slot
        , 'buy' as trade_type
        , base_amount_out as base_amount
        , max_quote_amount_in as max_min_sol_amount
        , account_pool as pool
        , account_user as user
        , account_user_base_token_account as user_base_token_account
        , account_user_quote_token_account as user_quote_token_account
        , account_pool_quote_token_account as pool_quote_token_account
        , account_protocol_fee_recipient as protocol_fee_recipient
        , account_protocol_fee_recipient_token_account as protocol_fee_recipient_token_account
        , call_outer_executing_account as outer_executing_account
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as swap_inner_index
        , call_tx_index as tx_index
        , 1 as is_buy
        , account_pool_quote_token_account as sol_target_account
        , NULL as sol_source_account
        , NULL as sol_dest_account
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('call_block_time')}}
    {% else %}
    WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    
    UNION ALL
    
    -- Sell operations
    SELECT
        call_block_time as block_time
        , call_block_slot as block_slot
        , 'sell' as trade_type
        , base_amount_in as base_amount
        , min_quote_amount_out as max_min_sol_amount
        , account_pool as pool
        , account_user as user
        , account_user_base_token_account as user_base_token_account
        , account_user_quote_token_account as user_quote_token_account
        , account_pool_quote_token_account as pool_quote_token_account
        , account_protocol_fee_recipient as protocol_fee_recipient
        , account_protocol_fee_recipient_token_account as protocol_fee_recipient_token_account
        , call_outer_executing_account as outer_executing_account
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as swap_inner_index
        , call_tx_index as tx_index
        , 0 as is_buy
        , NULL as sol_target_account
        , account_pool_quote_token_account as sol_source_account
        , account_user_quote_token_account as sol_dest_account
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('call_block_time')}}
    {% else %}
    WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

, fee_configs_with_time_ranges AS (
    SELECT 
        call_block_time as start_time,
        LEAD(call_block_time) OVER (ORDER BY call_block_time) as end_time,
        (lp_fee_basis_points + protocol_fee_basis_points) / 10000.0 AS total_fee_rate
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_update_fee_config') }}
)

, fee_configs_with_nulls_handled AS (
    SELECT
        start_time,
        COALESCE(end_time, TIMESTAMP '2099-12-31') as end_time,
        total_fee_rate
    FROM fee_configs_with_time_ranges
)

, swaps_with_fees AS (
    SELECT
        s.*,
        COALESCE(f.total_fee_rate, 0.0025) as total_fee_rate
    FROM swaps_base s
    LEFT JOIN fee_configs_with_nulls_handled f
        ON s.block_time >= f.start_time
        AND s.block_time < f.end_time
)

, swaps_with_transfers AS (
    SELECT
        sf.*,
        CASE
            WHEN sf.is_buy = 1 AND t.to_token_account = sf.sol_target_account THEN t.amount
            WHEN sf.is_buy = 0 AND t.from_token_account = sf.sol_source_account AND t.to_token_account = sf.sol_dest_account THEN t.amount
            ELSE null
        END as sol_amount
    FROM swaps_with_fees sf
    LEFT JOIN {{ ref('tokens_solana_transfers') }} t
        ON t.tx_id = sf.tx_id
        AND t.block_slot = sf.block_slot
        AND t.outer_instruction_index = sf.outer_instruction_index
        AND (
            (sf.swap_inner_index IS NULL AND (
                (sf.is_buy = 1 AND t.inner_instruction_index IN (1, 2, 3) AND t.to_token_account = sf.sol_target_account) OR
                (sf.is_buy = 0 AND t.inner_instruction_index IN (1, 2, 3) AND t.from_token_account = sf.sol_source_account AND t.to_token_account = sf.sol_dest_account)
            )) OR
            (sf.swap_inner_index IS NOT NULL AND (
                (sf.is_buy = 1 AND t.inner_instruction_index IN (sf.swap_inner_index + 1, sf.swap_inner_index + 2, sf.swap_inner_index + 3) AND t.to_token_account = sf.sol_target_account) OR
                (sf.is_buy = 0 AND t.inner_instruction_index IN (sf.swap_inner_index + 1, sf.swap_inner_index + 2, sf.swap_inner_index + 3) AND t.from_token_account = sf.sol_source_account AND t.to_token_account = sf.sol_dest_account)
            ))
        )
        AND t.token_mint_address = 'So11111111111111111111111111111111111111112'
        AND t.token_version = 'spl_token'
        {% if is_incremental() %}
        AND {{incremental_predicate('t.block_time')}}
        {% else %}
        AND t.block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
)

, trades_base as (
    SELECT
        sp.block_time
        , 'pumpswap' as project
        , 1 as version
        , 'solana' as blockchain
        , case 
            when sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' then 'direct'
            else sp.outer_executing_account
          end as trade_source
        --bought
        , case 
            when is_buy = 1 then p.baseMint  -- For buys, base token is bought
            else 'So11111111111111111111111111111111111111112'  -- For sells, SOL is bought
          end as token_bought_mint_address
        , case 
            when is_buy = 1 then CEIL(base_amount)  -- For buys, use base token amount
            else CAST(CEIL(sol_amount / (1 - sp.total_fee_rate)) AS DECIMAL(38,0))  -- For sells, calculate pre-fee SOL amount
          end as token_bought_amount_raw
        --sold
        , case 
            when is_buy = 0 then p.baseMint  -- For sells, base token is sold
            else 'So11111111111111111111111111111111111111112'  -- For buys, SOL is sold
          end as token_sold_mint_address
        , case 
            when is_buy = 0 then base_amount  -- For sells, base token is sold
            else sol_amount  -- For buys, SOL is sold
          end as token_sold_amount_raw
        , cast(sp.total_fee_rate as double) as fee_tier
        , cast(sp.pool as varchar) as pool_id
        , 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' as project_main_id
        , sp.user as trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.swap_inner_index
        , sp.tx_index
        , sp.block_slot
        , cast(case 
            when is_buy = 0 then sp.user_base_token_account
            else sp.user_quote_token_account
          end as varchar) as token_sold_vault
        , cast(case 
            when is_buy = 1 then sp.user_base_token_account
            else sp.user_quote_token_account
          end as varchar) as token_bought_vault
    FROM swaps_with_transfers sp
    LEFT JOIN pools p ON p.pool = sp.pool
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
    , tb.fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , tb.project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.swap_inner_index
    , tb.tx_index
FROM trades_base tb
