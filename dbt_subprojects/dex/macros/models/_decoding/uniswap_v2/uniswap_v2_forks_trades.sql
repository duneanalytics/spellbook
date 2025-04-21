{% macro uniswap_v2_forks_trades(
    dex_type = 'uni-v2'
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PairCreated = null
    )
%}

WITH evt_swap AS (
    SELECT
        blockchain
        , block_number
        , block_time
        , to
        , contract_address
        , tx_hash
        , evt_index
        , amount0In
        , amount0Out
        , amount1In
        , amount1Out
        , topic0 as pool_topic0
        , tx_from
        , tx_to
        , tx_index
    FROM {{ Pair_evt_Swap }}
)
-- filtering out bogus factory deployments
, factory_event_counts AS (
    SELECT 
        contract_address,
        pair,
        blockchain,
        COUNT(*) as event_count
    FROM {{ Factory_evt_PairCreated }}
    GROUP BY pair, blockchain, contract_address
    HAVING COUNT(*) = 1 -- Only keep pairs with exactly one factory event
)
, latest_creation_traces AS (
    SELECT 
        *
    FROM
        {{ ref('evms_latest_creation_trace') }}
)
, dexs AS
(
    SELECT
        t.blockchain
        , t.block_number
        , t.block_time
        , t.to AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        , CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.pool_topic0
        , t.tx_hash
        , t.evt_index
        , f.contract_address as factory_address
        , f.factory_topic0
        , f.factory_info
        , t.tx_from
        , t.tx_to
        , t.tx_index
    FROM
        evt_swap t
    INNER JOIN
        {{ Factory_evt_PairCreated }} f
        ON f.pair = t.contract_address
        AND f.blockchain = t.blockchain
    INNER JOIN
        factory_event_counts fec
        ON  fec.pair = f.pair
        AND fec.blockchain = f.blockchain
        AND fec.contract_address = f.contract_address
    INNER JOIN latest_creation_traces ct
        ON ct.address = f.pair
        AND ct."from" = f.contract_address
        AND ct.blockchain = f.blockchain
        AND ct.block_month = f.block_month
)

SELECT
    blockchain
    , '{{ version }}' AS version
    , '{{dex_type}}' AS dex_type
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , token_bought_amount_raw
    , token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , pool_topic0
    , tx_hash
    , evt_index
    , factory_address
    , factory_topic0
    , factory_info
    , tx_from
    , tx_to
    , tx_index
FROM dexs

{% endmacro %}
