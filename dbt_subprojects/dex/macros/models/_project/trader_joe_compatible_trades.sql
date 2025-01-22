{% macro trader_joe_compatible_v2_trades(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PoolCreated = null
    , pair_column_name = 'LBPair'
    )
%}
WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.recipient AS taker
        , t.contract_address AS maker
        , amountOut AS token_bought_amount_raw
        , amountIn AS token_sold_amount_raw
        , CASE WHEN swapForY = true THEN f.tokenY ELSE f.tokenX END AS token_bought_address -- when swapForY is true it means that tokenY is the token being bought else it's tokenX
        , CASE WHEN swapForY = true THEN f.tokenX ELSE f.tokenY END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM
        {{ Pair_evt_Swap }} t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
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

{% macro trader_joe_compatible_v2_1_trades(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PoolCreated = null
    , pair_column_name = 'LBPair'
    )
%}
WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.to AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN f.tokenX ELSE f.tokenY END AS token_bought_address
        , CASE WHEN amount0 < INT256 '0' THEN f.tokenY ELSE f.tokenX END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM (
        SELECT a.*
        ,CAST(bytearray_to_int256(bytearray_substring(amountsIn,17,32)) AS INT256) - CAST(bytearray_to_int256(bytearray_substring(amountsOut,17,32)) AS INT256) AS amount0
        ,CAST(bytearray_to_int256(bytearray_substring(amountsIn,1,16)) AS INT256) - CAST(bytearray_to_int256(bytearray_substring(amountsOut,1,16)) AS INT256) AS amount1
        FROM {{ Pair_evt_Swap }} a   
        ) t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
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