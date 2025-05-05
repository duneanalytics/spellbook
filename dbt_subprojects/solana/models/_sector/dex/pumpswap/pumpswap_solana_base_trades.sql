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
, swaps_with_safe_indices AS (
    SELECT 
        *,
        -- For direct calls, set swap_inner_index to outer_instruction_index
        CASE 
            WHEN outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' 
            THEN outer_instruction_index
            ELSE swap_inner_index
        END as safe_swap_index
    FROM swaps_with_fees
)
, base_token_transfers AS (
    SELECT
        t.tx_id,
        t.block_slot,
        t.outer_instruction_index,
        t.inner_instruction_index,
        t.from_token_account, 
        t.to_token_account,
        t.amount,
        t.token_mint_address
    FROM tokens_solana.transfers t
    WHERE t.block_time >= TIMESTAMP '2025-02-20'
)
, transfers_with_context AS (
    SELECT 
        t.*,
        CASE 
            WHEN t.token_mint_address = 'So11111111111111111111111111111111111111112' THEN 'quote'
            ELSE 'base'
        END as token_type,
        -- Flag fee transfers by checking if to_token_account matches protocol fee recipient
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM swaps_with_fees sf 
                WHERE sf.tx_id = t.tx_id 
                AND sf.outer_instruction_index = t.outer_instruction_index 
                AND sf.protocol_fee_recipient_token_account = t.to_token_account
            ) THEN 1
            ELSE 0
        END as is_fee
    FROM base_token_transfers t
)

, transfers_ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tx_id, outer_instruction_index, token_type
            ORDER BY inner_instruction_index ASC
        ) as rank_by_token_type
    FROM transfers_with_context
    WHERE is_fee = 0  -- Exclude fee transfers
)

, transfer_swap_mapping AS (
    SELECT 
        t.tx_id,
        t.outer_instruction_index,
        t.inner_instruction_index,
        t.token_type,
        t.amount,
        -- Find which swap this transfer belongs to
        MAX(sf.safe_swap_index) as associated_swap_index
    FROM transfers_ranked t
    JOIN swaps_with_safe_indices sf
        ON sf.tx_id = t.tx_id
        AND sf.outer_instruction_index = t.outer_instruction_index
        -- For direct calls, transfers are nested within the same instruction
        -- For nested calls, transfers must come after the instruction
        AND (
            (sf.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' AND t.outer_instruction_index = sf.safe_swap_index)
            OR 
            (sf.outer_executing_account != 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' AND t.inner_instruction_index > sf.safe_swap_index)
        )
    GROUP BY t.tx_id, t.outer_instruction_index, t.inner_instruction_index, t.token_type, t.amount
)
, transfer_by_swap AS (
    SELECT 
        tx_id,
        outer_instruction_index,
        associated_swap_index,
        token_type,
        -- Get first transfer of each type after each swap
        FIRST_VALUE(amount) OVER (
            PARTITION BY tx_id, outer_instruction_index, associated_swap_index, token_type
            ORDER BY inner_instruction_index ASC
        ) as transfer_amount
    FROM transfer_swap_mapping
)

, swaps_with_transfers AS (
    SELECT 
        sf.*,
        bt.transfer_amount as base_transfer_amount,
        qt.transfer_amount as quote_transfer_amount
    FROM swaps_with_safe_indices sf  -- Changed from swaps_with_fees
    LEFT JOIN (
        SELECT DISTINCT tx_id, outer_instruction_index, associated_swap_index, transfer_amount
        FROM transfer_by_swap
        WHERE token_type = 'base'
    ) bt ON bt.tx_id = sf.tx_id
        AND bt.outer_instruction_index = sf.outer_instruction_index
        AND bt.associated_swap_index = sf.safe_swap_index  -- Changed from swap_inner_index
    LEFT JOIN (
        SELECT DISTINCT tx_id, outer_instruction_index, associated_swap_index, transfer_amount
        FROM transfer_by_swap
        WHERE token_type = 'quote'
    ) qt ON qt.tx_id = sf.tx_id
        AND qt.outer_instruction_index = sf.outer_instruction_index
        AND qt.associated_swap_index = sf.safe_swap_index  -- Changed from swap_inner_index
)
, trades_base as (
    SELECT
        sp.block_time
        , sp.block_slot
        , 'pumpswap' as project
        , 1 as version
        , 'solana' as blockchain
        , sp.outer_executing_account as trade_source
        -- token bought amount - use only actual transfers, no fallback for direct calls
        , case 
            when is_buy = 1 then 
                CASE 
                    WHEN sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' THEN base_transfer_amount  -- Direct calls must have actual transfer
                    ELSE COALESCE(base_transfer_amount, base_amount)  -- Nested can fallback
                END
            else 
                CASE 
                    WHEN sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' THEN quote_transfer_amount  -- Direct calls must have actual transfer
                    ELSE COALESCE(quote_transfer_amount, sp.max_min_sol_amount)   -- Nested can fallback
                END
          end as token_bought_amount_raw
        -- token sold amount - use only actual transfers, no fallback for direct calls  
        , case 
            when is_buy = 0 then 
                CASE 
                    WHEN sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' THEN base_transfer_amount  -- Direct calls must have actual transfer
                    ELSE COALESCE(base_transfer_amount, base_amount)  -- Nested can fallback
                END
            else 
                CASE 
                    WHEN sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' THEN quote_transfer_amount  -- Direct calls must have actual transfer
                    ELSE COALESCE(quote_transfer_amount, sp.max_min_sol_amount)   -- Nested can fallback
                END
          end as token_sold_amount_raw
        , cast(sp.total_fee_rate as double) as fee_tier
        -- token bought mint address
        , case 
            when is_buy = 1 then p.baseMint  -- For buys, Crack token is bought
            else p.quoteMint  -- For sells, quote token is bought
          end as token_bought_mint_address
        -- token sold mint address
        , case 
            when is_buy = 0 then p.baseMint  -- For sells, base token is sold
            else 'So11111111111111111111111111111111111111112'  -- For buys, WSOL is sold (hardcoded since WSOL is always quote)
          end as token_sold_mint_address
        , cast(case 
            when is_buy = 0 then sp.user_base_token_account
            else sp.user_quote_token_account
          end as varchar) as token_sold_vault
        , cast(case 
            when is_buy = 1 then sp.user_base_token_account
            else sp.user_quote_token_account
          end as varchar) as token_bought_vault
        , cast(sp.pool as varchar) as pool_id
        , 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' as project_main_id
        , sp.user as trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.safe_swap_index as swap_inner_index
        , sp.tx_index
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
    , tb.swap_inner_index as inner_instruction_index
    , tb.tx_index
    , {{ dbt_utils.generate_surrogate_key(['tx_id', 'tx_index', 'outer_instruction_index', 'swap_inner_index']) }} as surrogate_key
FROM trades_base tb
