{% macro uniswap_v3_forks_trades(
    dex_type = 'uni-v3'
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PoolCreated = null
    , pair_column_name = 'pool'
    , taker_column_name = 'recipient'
    , maker_column_name = null
    )
%}

WITH evt_swap AS (
    SELECT
        blockchain
        , block_number
        , block_time
        , {{ taker_column_name }}
        {% if maker_column_name %}
        , {{ maker_column_name }}
        {% endif %}
        , amount0
        , amount1
        , contract_address
        , tx_hash
        , tx_index
        , evt_index
        , tx_from
        , tx_to
    FROM {{ Pair_evt_Swap }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

, dexs AS
(
    SELECT
        t.blockchain
        , t.block_number
        , t.block_time
        , t.{{ taker_column_name }} AS taker
        , {% if maker_column_name %}
                t.{{ maker_column_name }}
            {% else %}
                cast(null as varbinary)
            {% endif %} as maker
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
        , CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address as project_contract_address
        , t.tx_hash
        , t.evt_index
        , f.contract_address as factory_address
        , t.tx_from
        , t.tx_to
        , t.tx_index
    FROM
        evt_swap t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
        AND f.blockchain = t.blockchain
    INNER JOIN {{ source('evms', 'creation_traces') }} ct 
        ON f.{{ pair_column_name }} = ct.address 
        AND f.contract_address = ct."from"
        AND ct.blockchain = t.blockchain
)

SELECT
    blockchain
    , '{{ version }}' AS version
    , '{{dex_type}}' AS dex_type
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , CAST(token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    , CAST(token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , evt_index
    , factory_address
    , tx_from
    , tx_to
    , tx_index
FROM dexs
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
{% endmacro %}
