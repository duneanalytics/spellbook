{% macro uniswap_v2_forks_trades(
    blockchain = null
    , dex_type = 'uni-v2'
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PairCreated = null
    , pair_column_name = 'pair'
    )
%}

WITH evt_swap AS (
    SELECT
        {% if is_incremental() %}
        DISTINCT
        {% endif %}
        block_number
        , block_time
        , to
        , contract_address
        , tx_hash
        , index
        , amount0In
        , amount0Out
        , amount1In
        , amount1Out
    FROM {{ Pair_evt_Swap }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

, dexs AS
(
    SELECT
        t.block_number
        , t.block_time
        , t.to AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        , CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.tx_hash
        , t.index AS evt_index
        , f.contract_address as factory_address
    FROM
        evt_swap t
    INNER JOIN
        {{ Factory_evt_PairCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
    --some scammers emitted events with established pair addresses, joining in the the creation trace to resolve correct factory
    INNER JOIN {{ source(blockchain, 'creation_traces') }} ct 
        ON f.{{ pair_column_name }} = ct.address 
        AND f.contract_address = ct."from"
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{project}}' AS project
    , '{{ version }}' AS version
    , '{{dex_type}}' AS dex_type
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
    , dexs.factory_address
FROM
    dexs
     
{% endmacro %}