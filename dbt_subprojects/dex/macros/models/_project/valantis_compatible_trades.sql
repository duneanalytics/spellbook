{% macro valantis_compatible_hot_trades(
    blockchain = null
    , project = null
    , version = null
    , HOT_evt_Swap = null
    , Pair_evt_Swap = null
    , Factory_evt_PoolCreated = null
    )
%}
WITH dexs AS
(
    SELECT distinct t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.amountOut AS token_bought_amount_raw
        , t.amountIn AS token_sold_amount_raw
        , CASE WHEN t.isZeroToOne THEN f.token1 ELSE f.token0 END AS token_bought_address
        , CASE WHEN t.isZeroToOne THEN f.token0 ELSE f.token1 END AS token_sold_address
        , t.sender AS taker
        , coalesce(h.contract_address, t.contract_address) AS maker -- HOT for HOTSwaps (solver orders), SovereignPool for AMM swaps (permissionless)
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM {{ Pair_evt_Swap }} AS t
    INNER JOIN {{ Factory_evt_PoolCreated }} AS f
        ON t.contract_address = f.pool
    LEFT JOIN {{ HOT_evt_Swap }} AS h
        ON t.evt_block_number = h.evt_block_number AND t.evt_tx_hash = h.evt_tx_hash AND t.evt_index = h.evt_index + 3
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT '{{ blockchain }}' AS blockchain
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
