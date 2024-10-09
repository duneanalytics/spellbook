
{% macro uniswap_v3_forks_trades(
    blockchain = null
    , dex_type = 'uni-v3'
    , project = null
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
        block_number
        , block_time
        , {{ taker_column_name }}
        {% if maker_column_name %}
        , {{ maker_column_name }}
        {% endif %}
        , amount0
        , amount1
        , contract_address
        , tx_hash
        , index
    FROM {{ Pair_evt_Swap }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

, dexs AS
(
    SELECT
        t.block_number
        ,t.block_time
        , t.{{ taker_column_name }} AS taker
        , {% if maker_column_name %}
                t.{{ maker_column_name }}
            {% else %}
                cast(null as varbinary)
            {% endif %} as maker
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
        , CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address as project_contract_address
        , t.tx_hash
        , t.index as evt_index
        , f.contract_address as factory_address
    FROM
        evt_swap t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
    --some scammers emitted events with established pair addresses, joining in the the creation trace to resolve to correct factory
    INNER JOIN {{ source(blockchain, 'creation_traces') }} ct 
        ON f.{{ pair_column_name }} = ct.address 
        AND f.contract_address = ct."from"
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , '{{dex_type}}' AS dex_type
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
    , dexs.factory_address
FROM
    dexs
{% endmacro %}