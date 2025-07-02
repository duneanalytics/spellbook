-- inside the macro or a model
WITH all_swaps AS (

    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapExactTokensForTokens') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapExactTokensForETH') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapETHForExactTokens') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapTokensForExactTokens') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapTokensForExactETH') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapExactETHForTokens') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapexactethfortokenssupportingfeeontransfertokens') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapexacttokensforethsupportingfeeontransfertokens') }}
    UNION ALL
    SELECT * FROM {{ source('metropolis_sonic', 'router_call_swapexacttokensfortokenssupportingfeeontransfertokens') }}

)


{% macro metropolis_compatible_trades(
        blockchain = '',
        project = '',
        version = '',
        factory_create_pool_evt = null,
        token_swapped_function = 'all_swaps'
    )
%}

WITH 
-- Get swap events
swap_events AS (
    SELECT
        buyer,
        contract_address,
        evt_block_number,
        evt_block_time,
        evt_tx_hash,
        evt_index,
        element_at(amounts, 1) AS amount0,
        element_at(amounts, 2) AS amount1,
        swapAmount,
        feeAmount
    FROM {{ token_swapped_function }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

-- Determine swap direction based on swap amount and amounts array
swap_direction AS (
    SELECT
        s.*,
        -- If swapAmount equals abs(amount0), then token0 is being sold (token0 = tokenIn)
        -- Otherwise, token1 is being sold (token1 = tokenIn)
        CASE
            WHEN ABS(amount0) = swapAmount THEN 0  -- token0 is being sold
            ELSE 1                                 -- token1 is being sold
        END AS tokenIn,
        CASE
            WHEN ABS(amount0) = swapAmount THEN 1  -- token1 is being bought
            ELSE 0                                 -- token0 is being bought
        END AS tokenOut,
        -- Determine the absolute amounts
        ABS(amount0) AS amount0_abs,
        ABS(amount1) AS amount1_abs
    FROM swap_events s
),

-- Map the swap events to token information
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
        s.contract_address AS project_contract_address,
        s.evt_block_number AS block_number,
        s.evt_block_time AS block_time,
        s.evt_tx_hash AS tx_hash,
        s.evt_index
    FROM swap_direction s
    LEFT JOIN (SELECT * FROM unique_pool_tokens WHERE rn = 1) p 
        ON s.contract_address = p.contract_address
    -- Filter out swaps where we couldn't map the token addresses
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
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
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
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs

{% endmacro %}













