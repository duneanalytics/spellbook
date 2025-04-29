{{
  config(
    schema = 'pumpswap_solana',
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

{% set project_start_date = '2025-03-14' %}

WITH pools AS (
    SELECT
        pool,
        baseMint,
        quoteMint,
        baseMintDecimals,
        quoteMintDecimals
    FROM {{ ref('pumpswap_solana_pools') }}
),

-- Add block_month early and deduplicate
swaps_base AS (
    SELECT
        *,
        CAST(date_trunc('month', block_time) AS DATE) as block_month
    FROM (
        -- Buy operations
        SELECT
            call_block_time as block_time,
            call_block_slot as block_slot,
            'buy' as trade_type,
            base_amount_out as base_amount,
            max_quote_amount_in as max_min_sol_amount,
            account_pool as pool,
            account_user as user,
            account_user_base_token_account as user_base_token_account,
            account_user_quote_token_account as user_quote_token_account,
            account_pool_quote_token_account as pool_quote_token_account,
            account_protocol_fee_recipient as protocol_fee_recipient,
            account_protocol_fee_recipient_token_account as protocol_fee_recipient_token_account,
            call_outer_executing_account as outer_executing_account,
            call_tx_id as tx_id,
            call_outer_instruction_index as outer_instruction_index,
            COALESCE(call_inner_instruction_index, 0) as inner_instruction_index,
            call_tx_index as tx_index,
            1 as is_buy,
            account_pool_quote_token_account as sol_target_account,
            NULL as sol_source_account,
            NULL as sol_dest_account
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('call_block_time')}}
        {% else %}
        WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        
        UNION ALL
        
        -- Sell operations
        SELECT
            call_block_time as block_time,
            call_block_slot as block_slot,
            'sell' as trade_type,
            base_amount_in as base_amount,
            min_quote_amount_out as max_min_sol_amount,
            account_pool as pool,
            account_user as user,
            account_user_base_token_account as user_base_token_account,
            account_user_quote_token_account as user_quote_token_account,
            account_pool_quote_token_account as pool_quote_token_account,
            account_protocol_fee_recipient as protocol_fee_recipient,
            account_protocol_fee_recipient_token_account as protocol_fee_recipient_token_account,
            call_outer_executing_account as outer_executing_account,
            call_tx_id as tx_id,
            call_outer_instruction_index as outer_instruction_index,
            COALESCE(call_inner_instruction_index, 0) as inner_instruction_index,
            call_tx_index as tx_index,
            0 as is_buy,
            NULL as sol_target_account,
            account_pool_quote_token_account as sol_source_account,
            account_user_quote_token_account as sol_dest_account
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('call_block_time')}}
        {% else %}
        WHERE call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
    )
),

-- Deduplicate early using block_month
swaps_deduplicated AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    tx_id, 
                    outer_instruction_index, 
                    inner_instruction_index, 
                    tx_index, 
                    block_month
                ORDER BY block_time DESC
            ) as row_num
        FROM swaps_base
    ) s
    WHERE s.row_num = 1
),

swaps_with_transfers AS (
    SELECT
        s.*,
        CASE
            WHEN s.is_buy = 1 AND t.to_token_account = s.sol_target_account THEN t.amount
            WHEN s.is_buy = 0 AND t.from_token_account = s.sol_source_account AND t.to_token_account = s.sol_dest_account THEN t.amount
            ELSE s.max_min_sol_amount
        END as sol_amount
    FROM swaps_deduplicated s
    LEFT JOIN {{ ref('tokens_solana_transfers') }} t
        ON t.tx_id = s.tx_id 
        AND t.action = 'transfer'
        AND t.outer_instruction_index = s.outer_instruction_index
        AND t.token_mint_address = 'So11111111111111111111111111111111111111112'
        AND t.token_version = 'spl_token'
        AND ((s.is_buy = 1 AND t.to_token_account = s.sol_target_account) OR
             (s.is_buy = 0 AND t.from_token_account = s.sol_source_account AND t.to_token_account = s.sol_dest_account))
        {% if is_incremental() %}
        AND {{incremental_predicate('t.block_time')}}
        {% else %}
        AND t.block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
),

trades_base as (
    SELECT
        sp.block_time,
        'pumpswap' as project,
        1 as version,
        'solana' as blockchain,
        case 
            when sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' then 'direct'
            else sp.outer_executing_account
        end as trade_source,
        --bought
        case 
            when is_buy = 1 then p.baseMint
            else 'So11111111111111111111111111111111111111112'
        end as token_bought_mint_address,
        case 
            when is_buy = 1 then base_amount
            else sol_amount
        end as token_bought_amount_raw,
        --sold
        case 
            when is_buy = 0 then p.baseMint
            else 'So11111111111111111111111111111111111111112'
        end as token_sold_mint_address,
        case 
            when is_buy = 0 then base_amount
            else sol_amount
        end as token_sold_amount_raw,
        cast(sp.pool as varchar) as pool_id,
        'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' as project_main_id,
        sp.user as trader_id,
        sp.tx_id,
        sp.outer_instruction_index,
        sp.inner_instruction_index,
        sp.tx_index,
        sp.block_slot,
        cast(case 
            when is_buy = 0 then sp.user_base_token_account
            else sp.user_quote_token_account
        end as varchar) as token_sold_vault,
        cast(case 
            when is_buy = 1 then sp.user_base_token_account
            else sp.user_quote_token_account
        end as varchar) as token_bought_vault,
        sp.block_month,
        -- Create surrogate key for internal use
        {{ dbt_utils.generate_surrogate_key([
            'sp.tx_id',
            'sp.outer_instruction_index',
            'sp.inner_instruction_index',
            'sp.tx_index',
            'cast(sp.block_month as varchar)'
        ]) }} as trade_id
    FROM swaps_with_transfers sp
    LEFT JOIN pools p ON p.pool = sp.pool
)

SELECT
    tb.blockchain,
    tb.project,
    tb.version,
    tb.block_month,
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
    tb.tx_index,
    tb.trade_id
FROM trades_base tb