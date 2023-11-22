{% macro uniswap_compatible_v2_trades(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PairCreated = null
    )
%}
WITH dexs AS
(
    -- Uniswap v2
    -- Swap events contain amounts in and out for both tokens 0 and 1, so we actually have two "swaps" in each event:
    -- * Token 0 in, token 1 out
    -- * Token 1 in, token 0 out
    -- Most of the time, one of these swaps has empty/zero amounts.
    -- However, there are some edge cases where both amounts for one of the tokens is non-zero, and both amounts for the other token are zero. eg 0x2643cc80871c4134704863b17f10901ea6d11646fc10162ff1fb504e7137c193
    -- and others where both amounts for both tokens are non-zero eg 0x5afc81f74fbfca62c7d083cccbadd01158f65bfdb1f2c7fb24e4866c1d96e346
    -- We need to handle both of these cases, so we use a UNION ALL to get both swaps from each event, but avoiding cluttering the dataset with empty swaps (0 for 0)
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.to AS taker
        , t.contract_address AS maker
        , amount0Out AS token_bought_amount_raw
        , amount1In AS token_sold_amount_raw
        , f.token0 AS token_bought_address
        , f.token1 AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM
        {{ Pair_evt_Swap }} t
    INNER JOIN
        {{ Factory_evt_PairCreated }} f
        ON f.pair = t.contract_address
    WHERE (amount0Out > UINT256 '0' OR amount1In > UINT256 '0')
    {% if is_incremental() %}
        AND {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}

    UNION ALL

    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.to AS taker
        , t.contract_address AS maker
        , amount1Out AS token_bought_amount_raw
        , amount0In AS token_sold_amount_raw
        , f.token1 AS token_bought_address
        , f.token0 AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM
        {{ Pair_evt_Swap }} t
    INNER JOIN
        {{ Factory_evt_PairCreated }} f
        ON f.pair = t.contract_address
    WHERE (amount1Out > UINT256 '0' OR amount0In > UINT256 '0')
    {% if is_incremental() %}
        AND {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs
{% endmacro %}

{% macro uniswap_compatible_v3_trades(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PoolCreated = null
    )
%}
WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.recipient AS taker
        , CAST(NULL as VARBINARY) as maker
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
        , CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address as project_contract_address
        , f.fee
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
    FROM
        {{ Pair_evt_Swap }} t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.pool = t.contract_address
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    , CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs
{% endmacro %}