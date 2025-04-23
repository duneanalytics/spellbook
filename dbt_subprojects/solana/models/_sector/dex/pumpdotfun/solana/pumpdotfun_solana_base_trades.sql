{{
  config(
        schema = 'pumpdotfun_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'block_month'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}')
}}

{% set project_start_date = '2024-01-14' %} --grabbed program deployed at time (account created at)

with
    bonding_curves as (
        SELECT
            account_base_mint as token_mint_address
            , account_pool as bonding_curve
            , account_pool_base_token_account as bonding_curve_vault
            , base_amount_in as initial_token_reserves_raw 
            , quote_amount_in as initial_sol_reserves_raw 
            , call_block_time as block_time
            , call_tx_id as tx_id
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_create_pool') }}
        WHERE 1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        --AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        AND call_block_time >= now() - interval '7' day  -- add this line
        {% endif %}
    )

    , buy_swaps as (
        SELECT
            account_base_mint as token_mint_address
            , max_quote_amount_in as sol_amount
            , base_amount_out as token_amount
            , account_user as user
            , account_pool as pool_address
            , call_block_time as trade_timestamp
            , call_tx_id as tx_id
            , call_tx_index as tx_index
            , call_block_time as block_time
            , call_block_slot as block_slot
            , call_outer_instruction_index as outer_instruction_index
            , call_inner_instruction_index as inner_instruction_index
            , call_outer_executing_account as outer_executing_account
            , 1 as is_buy
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }}
        WHERE 1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        --AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        AND call_block_time >= now() - interval '7' day  -- add this line
        {% endif %}
    )

    , sell_swaps as (
        SELECT
            account_base_mint as token_mint_address
            , min_quote_amount_out as sol_amount
            , base_amount_in as token_amount
            , account_user as user
            , account_pool as pool_address
            , call_block_time as trade_timestamp
            , call_tx_id as tx_id
            , call_tx_index as tx_index
            , call_block_time as block_time
            , call_block_slot as block_slot
            , call_outer_instruction_index as outer_instruction_index
            , call_inner_instruction_index as inner_instruction_index
            , call_outer_executing_account as outer_executing_account
            , 0 as is_buy
        FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
        WHERE 1=1
        {% if is_incremental() %}
        AND {{incremental_predicate('call_block_time')}}
        {% else %}
        --AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        AND call_block_time >= now() - interval '7' day  -- add this line
        {% endif %}
    )

    -- Track impact of each trade on reserves
    , swaps_with_impact as (
        SELECT 
            *,
            CASE 
                WHEN is_buy = 1 THEN -token_amount -- Buy: token reserves decrease
                ELSE token_amount  -- Sell: token reserves increase
            END AS token_reserves_impact,
            CASE 
                WHEN is_buy = 1 THEN sol_amount -- Buy: SOL reserves increase
                ELSE -sol_amount    -- Sell: SOL reserves decrease
            END AS sol_reserves_impact
        FROM (
            SELECT * FROM buy_swaps
            UNION ALL
            SELECT * FROM sell_swaps
        )
    )

-- Combine initial states with trades for reserves calculation
    , reserve_events as (
        -- Initial pool creation events
        SELECT
            token_mint_address,
            bonding_curve as pool_address,
            block_time as event_time,
            tx_id,
            initial_token_reserves_raw as token_reserves_change,
            initial_sol_reserves_raw as sol_reserves_change,
            0 as event_order -- Pool creation comes first for same timestamp
        FROM bonding_curves

        UNION ALL

        -- Trade events
        SELECT
            token_mint_address,
            pool_address,
            trade_timestamp as event_time,
            tx_id,
            token_reserves_impact as token_reserves_change,
            sol_reserves_impact as sol_reserves_change,
            1 as event_order -- Trades come after pool creation for same timestamp
        FROM swaps_with_impact
    )

    -- Calculate running totals for reserves
    , running_reserves as (
        SELECT
            token_mint_address,
            pool_address,
            event_time,
            tx_id,
            event_order,
            SUM(token_reserves_change) OVER (
                PARTITION BY pool_address 
                ORDER BY event_time, event_order, tx_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as token_reserves_raw,
            SUM(sol_reserves_change) OVER (
                PARTITION BY pool_address 
                ORDER BY event_time, event_order, tx_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as sol_reserves_raw
        FROM reserve_events
    )

    -- Join swaps with the latest reserves value
    , swaps_with_reserves as (
        SELECT      
            s.*,
            COALESCE(r.token_reserves_raw, 0) as token_reserves,
            COALESCE(r.sol_reserves_raw, 0) as sol_reserves
        FROM swaps_with_impact s
        LEFT JOIN (
            SELECT DISTINCT ON (pool_address, tx_id)  -- Take only one record per pool and tx
                pool_address,
                tx_id,
                token_reserves_raw,
                sol_reserves_raw
            FROM running_reserves
            ORDER BY pool_address, tx_id, event_time DESC, event_order DESC  -- Take the latest state
        ) r ON s.pool_address = r.pool_address AND s.tx_id = r.tx_id
    )

    , trades_base as (
        SELECT
            sp.block_time
            , 'pumpdotfun' as project
            , 1 as version
            , 'solana' as blockchain
            , case when sp.outer_executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' then 'direct'
                else sp.outer_executing_account
                end as trade_source
            --bought
            , case when is_buy = 1 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
                end as token_bought_mint_address
            , case when is_buy = 1 then token_amount
                else sol_amount
                end as token_bought_amount_raw
            --sold
            , case when is_buy = 0 then sp.token_mint_address
                else 'So11111111111111111111111111111111111111112'
                end as token_sold_mint_address
            , case when is_buy = 0 then token_amount
                else sol_amount
                end as token_sold_amount_raw
            , cast(bc.bonding_curve as varchar) as pool_id
            , '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' as project_main_id
            , sp.sol_reserves as sol_reserves_raw
            , sp.sol_reserves/pow(10,COALESCE(tk_sol.decimals, 9)) as sol_reserves
            , sp.token_reserves as token_reserves_raw
            , sp.token_reserves/pow(10,COALESCE(tk.decimals, 0)) as token_reserves
            , sp.user as trader_id
            , sp.tx_id
            , sp.outer_instruction_index
            , sp.inner_instruction_index
            , sp.tx_index
            , sp.block_slot
            , cast(case when is_buy = 1 then bc.bonding_curve --sol is just held on the curve account
                else bonding_curve_vault
                end as varchar) as token_bought_vault
            , cast(case when is_buy = 0 then bc.bonding_curve --sol is just held on the curve account
                else bonding_curve_vault
                end as varchar) as token_sold_vault
            , ROW_NUMBER() OVER (
                PARTITION BY 
                    sp.tx_id, 
                    sp.outer_instruction_index, 
                    COALESCE(sp.inner_instruction_index, 0), 
                    CAST(date_trunc('month', sp.block_time) AS DATE)
                ORDER BY 
                    sp.block_time,
                    sp.block_slot
            ) as row_num
        FROM swaps_with_reserves sp
        LEFT JOIN {{ ref('tokens_solana_fungible') }} tk ON tk.token_mint_address = sp.token_mint_address
        LEFT JOIN {{ ref('tokens_solana_fungible') }} tk_sol ON tk_sol.token_mint_address = 'So11111111111111111111111111111111111111112'
        LEFT JOIN bonding_curves bc ON bc.token_mint_address = sp.token_mint_address
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
    , cast(0.01 as double) as fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.sol_reserves_raw
    , tb.sol_reserves
    , tb.token_reserves_raw
    , tb.token_reserves
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , tb.project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
FROM trades_base tb
WHERE tb.row_num = 1  -- Only keep the first row of each duplicate group