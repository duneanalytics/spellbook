{% macro saru_compatible_v2_trades(
    blockchain = null,
    project = null,
    version = null,
    Pair_evt_Swap = null,
    Pair_call_token0 = null,
    Pair_call_token1 = null
) %}
WITH token0_calls AS (
    SELECT
        contract_address AS pool_address,
        output_0 AS token0_address,
        call_block_time,
        call_block_number
    FROM {{ Pair_call_token0 }}
    WHERE call_success = TRUE
),
latest_token0 AS (
    SELECT
        pool_address,
        token0_address,
        ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY call_block_number DESC, call_block_time DESC) AS rn
    FROM token0_calls
),
token1_calls AS (
    SELECT
        contract_address AS pool_address,
        output_0 AS token1_address,
        call_block_time,
        call_block_number
    FROM {{ Pair_call_token1 }}
    WHERE call_success = TRUE
),
latest_token1 AS (
    SELECT
        pool_address,
        token1_address,
        ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY call_block_number DESC, call_block_time DESC) AS rn
    FROM token1_calls
),
pool_tokens AS (
    SELECT
        t0.pool_address,
        t0.token0_address,
        t1.token1_address
    FROM latest_token0 AS t0
    JOIN latest_token1 AS t1
        ON t0.pool_address = t1.pool_address
    WHERE t0.rn = 1 AND t1.rn = 1
),
swap_events AS (
    SELECT
        evt_block_number AS block_number,
        TRY_CAST(evt_block_time AS TIMESTAMP(3) WITH TIME ZONE) AS block_time,
        evt_tx_from AS maker,
        contract_address AS pool_address,
        COALESCE(amount0In, 0) AS amount0_in,
        COALESCE(amount1In, 0) AS amount1_in,
        COALESCE(amount0Out, 0) AS amount0_out,
        COALESCE(amount1Out, 0) AS amount1_out,
        sender,
        to,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ Pair_evt_Swap }}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    TRY_CAST(DATE_TRUNC('month', se.block_time) AS DATE) AS block_month,
    TRY_CAST(DATE_TRUNC('day', se.block_time) AS DATE) AS block_date,
    se.block_time,
    se.block_number,
    CASE WHEN se.amount0_in > 0 THEN se.amount0_in ELSE se.amount1_in END AS token_sold_amount_raw,
    CASE WHEN se.amount0_out > 0 THEN se.amount0_out ELSE se.amount1_out END AS token_bought_amount_raw,
    CASE WHEN se.amount0_in > 0 THEN pt.token0_address ELSE pt.token1_address END AS token_sold_address,
    CASE WHEN se.amount0_out > 0 THEN pt.token0_address ELSE pt.token1_address END AS token_bought_address,
    se.maker,
    se.to AS taker,
    se.pool_address AS project_contract_address,
    se.tx_hash,
    se.evt_index
FROM swap_events AS se
JOIN pool_tokens AS pt
    ON se.pool_address = pt.pool_address
{% endmacro %}
