{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-02-20' %}

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
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient
        , account_protocol_fee_recipient_token_account
        , call_outer_executing_account as outer_executing_account
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as swap_inner_index
        , call_tx_index as tx_index
        , 1 as is_buy
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
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient
        , account_protocol_fee_recipient_token_account
        , call_outer_executing_account as outer_executing_account
        , call_tx_id as tx_id
        , call_outer_instruction_index as outer_instruction_index
        , call_inner_instruction_index as swap_inner_index
        , call_tx_index as tx_index
        , 0 as is_buy
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
        sf.base_amount as base_token_amount, 
        t.amount as quote_token_amount ,
        ROW_NUMBER() OVER (
                    PARTITION BY sf.tx_id, sf.outer_instruction_index, sf.swap_inner_index
                    ORDER BY t.inner_instruction_index ASC 
                ) as rn
    FROM swaps_with_fees sf
    INNER JOIN {{ source('tokens_solana','transfers') }} t
        ON t.tx_id = sf.tx_id
        AND t.block_slot = sf.block_slot
        AND t.outer_instruction_index = sf.outer_instruction_index
        AND t.to_token_account != sf.account_protocol_fee_recipient_token_account
        AND (
            (sf.swap_inner_index IS NULL 
            AND t.inner_instruction_index BETWEEN 1 AND 12
            AND (
                        CASE 
                            WHEN sf.is_buy = 1 THEN t.from_token_account = sf.account_user_quote_token_account
                            ELSE t.from_token_account = sf.account_pool_quote_token_account
                        END
                    )   
            ) 
            OR
            (sf.swap_inner_index IS NOT NULL 
            AND t.inner_instruction_index > sf.swap_inner_index
            AND t.inner_instruction_index BETWEEN sf.swap_inner_index + 1 AND sf.swap_inner_index + 12
            AND (
                        CASE 
                            WHEN sf.is_buy = 1 THEN t.from_token_account = sf.account_user_quote_token_account
                            ELSE t.from_token_account = sf.account_pool_quote_token_account
                        END
                    )  
            )
        )
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
            when is_buy = 1 then p.baseMint 
            else p.quoteMint 
          end as token_bought_mint_address
        , case when sp.is_buy = 1 then sp.base_token_amount else sp.quote_token_amount end AS token_bought_amount_raw
        --sold
        , case 
            when is_buy = 0 then p.baseMint 
            else p.quoteMint 
          end as token_sold_mint_address
        , case when sp.is_buy = 0 then sp.base_token_amount else sp.quote_token_amount end AS token_sold_amount_raw
        , cast(sp.total_fee_rate as double) as fee_tier
        , sp.pool as pool_id
        , 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' as project_main_id
        , sp.user as trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.swap_inner_index as inner_instruction_index
        , sp.tx_index
        , sp.block_slot
        , case
            when sp.is_buy = 1 then sp.account_pool_base_token_account
            else sp.account_pool_quote_token_account
          end as token_bought_vault
        , case
            when sp.is_buy = 1 then sp.account_pool_quote_token_account
            else sp.account_pool_base_token_account
          end as token_sold_vault
    FROM swaps_with_transfers sp
    LEFT JOIN pools p ON p.pool = sp.pool
    WHERE sp.rn = 1 
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
    , coalesce(tb.inner_instruction_index, 0) as inner_instruction_index --coalesce to 0 to avoid null uniqueness error
    , tb.tx_index
    , {{ dbt_utils.generate_surrogate_key(['tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']) }} as surrogate_key
FROM trades_base tb