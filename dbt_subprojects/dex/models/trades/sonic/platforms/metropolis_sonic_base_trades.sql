{{ config(
    schema = 'metropolis_sonic',
    alias  = 'base_trades',
    materialized = 'incremental',
    file_format  = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

WITH all_swaps AS (

    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, amountIn, amountOutMin, deadline, output_amounts, path, to FROM {{ source('metropolis_sonic', 'router_call_swapexacttokensfortokens') }}
    UNION ALL
    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, amountIn, amountOutMin, deadline, output_amounts, path, to FROM {{ source('metropolis_sonic', 'router_call_swapexacttokensforeth') }}
    UNION ALL
    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, NULL, amountOut as amountOutMin, deadline, output_amounts, path, to FROM {{ source('metropolis_sonic', 'router_call_swapethforexacttokens') }}
    UNION ALL
    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, amountInMax as amountIn, amountOut as amountOutMin, deadline, output_amounts, path, to FROM {{ source('metropolis_sonic', 'router_call_swaptokensforexacttokens') }}
    UNION ALL
    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, NULL, amountOutMin, deadline, output_amounts, path, to FROM {{ source('metropolis_sonic', 'router_call_swapexactethfortokens') }}
    UNION ALL
    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, NULL, amountOutMin, deadline, NULL, path, to  FROM {{ source('metropolis_sonic', 'router_call_swapexactethfortokenssupportingfeeontransfertokens') }}
    UNION ALL
    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, amountIn, amountOutMin, deadline, NULL, path, to FROM {{ source('metropolis_sonic', 'router_call_swapexacttokensforethsupportingfeeontransfertokens') }}
    UNION ALL
    SELECT contract_address, call_success, call_tx_hash, call_tx_from, call_tx_to, call_tx_index, call_trace_address, call_block_time, call_block_number, call_block_date, amountIn, amountOutMin, deadline, NULL, path, to  FROM {{ source('metropolis_sonic', 'router_call_swapexacttokensfortokenssupportingfeeontransfertokens') }}

),

WITH 
swap_events AS (
    SELECT
        contract_address,
        call_block_number,
        call_block_time,
        call_tx_hash,
        call_tx_index,
        call_trace_address,
        call_success,
        call_tx_from,
        call_tx_to,
        call_block_date,
        amountIn,
        amountOutMin,
        deadline,
        output_amounts,
        path,
        to
    FROM all_swaps
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {% endif %}
),

swap_direction AS (
    SELECT
        s.*,
        -- Use amountIn as the swap amount for inference
        0 AS tokenIn,  -- You can refine this logic based on path[1] etc. later
        1 AS tokenOut,
        amountIn AS amount0_abs,
        element_at(output_amounts, cardinality(output_amounts)) AS amount1_abs
    FROM swap_events s
),

dexs AS (
    SELECT
        CASE 
            WHEN s.tokenIn = 0 THEN p.token0 
            WHEN s.tokenIn = 1 THEN p.token1
            ELSE CAST(NULL AS VARBINARY)
        END AS token_sold_address,
        CASE
            WHEN s.tokenIn = 0 THEN s.amount0_abs
            WHEN s.tokenIn = 1 THEN s.amount1_abs
            ELSE CAST(NULL AS uint256)
        END AS token_sold_amount_raw,
        CASE 
            WHEN s.tokenOut = 0 THEN p.token0 
            WHEN s.tokenOut = 1 THEN p.token1
            ELSE CAST(NULL AS VARBINARY)
        END AS token_bought_address,
        CASE
            WHEN s.tokenOut = 0 THEN s.amount0_abs
            WHEN s.tokenOut = 1 THEN s.amount1_abs
            ELSE CAST(NULL AS uint256)
        END AS token_bought_amount_raw,
        s.contract_address,
        s.call_block_number AS block_number,
        s.call_block_time AS block_time,
        s.call_tx_hash AS tx_hash,
        s.call_tx_index AS evt_index,
        s.call_success,
        s.call_tx_from,
        s.call_tx_to,
        s.call_trace_address,
        s.call_block_date,
        s.amountIn,
        s.amountOutMin,
        s.deadline,
        s.output_amounts,
        s.path,
        s.to
    FROM swap_direction s
    LEFT JOIN (SELECT * FROM unique_pool_tokens WHERE rn = 1) p 
        ON s.contract_address = p.contract_address
    WHERE 
        CASE 
            WHEN s.tokenIn = 0 THEN p.token0 
            WHEN s.tokenIn = 1 THEN p.token1
            ELSE CAST(NULL AS VARBINARY)
        END IS NOT NULL
        AND
        CASE 
            WHEN s.tokenOut = 0 THEN p.token0 
            WHEN s.tokenOut = 1 THEN p.token1
            ELSE CAST(NULL AS VARBINARY)
        END IS NOT NULL
)

SELECT
    'sonic' AS blockchain,
    'metropolis' AS project,
    '1' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    CAST(NULL AS VARBINARY) AS taker,
    CAST(NULL AS VARBINARY) AS maker,
    dexs.contract_address AS project_contract_address,
    dexs.tx_hash,
    dexs.evt_index,
    dexs.call_success,
    dexs.call_tx_hash,
    dexs.call_tx_from,
    dexs.call_tx_to,
    dexs.call_tx_index,
    dexs.call_trace_address,
    dexs.call_block_time,
    dexs.call_block_number,
    dexs.call_block_date,
    dexs.amountIn,
    dexs.amountOutMin,
    dexs.deadline,
    dexs.output_amounts,
    dexs.path,
    dexs.to
FROM dexs
