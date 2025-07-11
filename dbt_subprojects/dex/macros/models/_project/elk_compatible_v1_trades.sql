{% macro elk_compatible_v1_trades(
    blockchain = null,
    project = null,
    version = null,
    Pair_evt_Swap = null,
    Factory_evt_PairCreated = null,
    pair_column_name = 'pair'
) %}
WITH dexs AS (
    SELECT
        s.evt_block_number AS block_number,
        CAST(s.evt_block_time AS timestamp(3) WITH time zone) AS block_time,
        s.evt_tx_from AS maker,
        s.to AS taker,

        -- Raw token amounts
        s.amount0In,
        s.amount0Out,
        s.amount1In,
        s.amount1Out,

        -- Define token sold/bought amounts
        CASE 
            WHEN s.amount0In > 0 THEN s.amount0In
            ELSE s.amount1In
        END AS token_sold_amount_raw,

        CASE 
            WHEN s.amount0Out > 0 THEN s.amount0Out
            ELSE s.amount1Out
        END AS token_bought_amount_raw,

        f.token0,
        f.token1,

        -- Token addresses based on the amounts
        CASE 
            WHEN s.amount0In > 0 THEN f.token0
            ELSE f.token1
        END AS token_sold_address,

        CASE 
            WHEN s.amount0Out > 0 THEN f.token0
            ELSE f.token1
        END AS token_bought_address,

        CAST(s.contract_address AS varbinary) AS project_contract_address,
        s.evt_tx_hash AS tx_hash,
        s.evt_index AS evt_index
    FROM 
        {{ Pair_evt_Swap }} s
    LEFT JOIN 
        {{ Factory_evt_PairCreated }} f
        ON s.contract_address = f.{{ pair_column_name }}
    {% if is_incremental() %}
    WHERE 
        {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
    block_time,
    block_number,
    token_sold_amount_raw,
    token_bought_amount_raw,
    token_sold_address,
    token_bought_address,
    maker,
    taker,
    project_contract_address,
    tx_hash,
    evt_index
FROM dexs
{% endmacro %}
