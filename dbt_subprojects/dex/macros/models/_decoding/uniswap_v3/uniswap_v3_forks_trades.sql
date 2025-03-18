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
        , pool_topic0
        , tx_hash
        , tx_index
        , evt_index
        , tx_from
        , tx_to
    FROM {{ Pair_evt_Swap }}
)
-- filtering out bogus factory deployments
, factory_event_counts AS (
    SELECT 
        contract_address,
        {{ pair_column_name }},
        blockchain,
        COUNT(*) as event_count
    FROM {{ Factory_evt_PoolCreated }}
    GROUP BY {{ pair_column_name }}, blockchain, contract_address
    HAVING COUNT(*) = 1 -- Only keep pools with exactly one factory event
)
, latest_creation_traces AS (
    SELECT 
        *
    FROM
        {{ ref('evms_latest_creation_trace') }}
)
-- Regular trades with normal filtering
, regular_trades AS (
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
        , t.pool_topic0
        , t.tx_hash
        , t.evt_index
        , f.contract_address as factory_address
        , f.factory_info
        , f.factory_topic0
        , t.tx_from
        , t.tx_to
        , t.tx_index
    FROM
        evt_swap t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
        AND f.blockchain = t.blockchain
    INNER JOIN
        factory_event_counts fec
        ON fec.{{ pair_column_name }} = f.{{ pair_column_name }}
        AND fec.blockchain = f.blockchain
        AND fec.contract_address = f.contract_address
    INNER JOIN latest_creation_traces ct
        ON ct.address = f.{{ pair_column_name }}
        AND ct."from" = f.contract_address
        AND ct.blockchain = f.blockchain
        AND ct.block_month = f.block_month
)
, optimism_mapped_trades AS (
     -- Special handling for Optimism trades in with mappings (bypassing some filters)
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
        , t.pool_topic0
        , t.tx_hash
        , t.evt_index
        , f.contract_address as factory_address
        , f.factory_info
        , f.factory_topic0
        , t.tx_from
        , t.tx_to
        , t.tx_index
    FROM
        evt_swap t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
        AND f.blockchain = t.blockchain
    INNER JOIN {{ ref('uniswap_optimism_ovm1_pool_mapping') }} ov
        ON t.contract_address = ov.newaddress
    WHERE t.blockchain = 'optimism'
        AND f.contract_address = 0x1F98431c8aD98523631AE4a59f267346ea31F984
)
-- Combine regular trades and special Optimism trades
, combined_trades AS (
    SELECT * 
    FROM regular_trades
    UNION ALL
    -- Only include Optimism mapped trades that aren't already in regular_trades
    SELECT o.* 
    FROM optimism_mapped_trades o
    LEFT JOIN regular_trades r
        ON o.blockchain = r.blockchain
        AND o.tx_hash = r.tx_hash
        AND o.block_number = r.block_number
        AND o.tx_index = r.tx_index
        AND o.evt_index = r.evt_index
        AND o.project_contract_address = r.project_contract_address
    WHERE r.tx_hash IS NULL
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
    , pool_topic0
    , tx_hash
    , evt_index
    , factory_address
    , factory_info
    , factory_topic0
    , tx_from
    , tx_to
    , tx_index
FROM combined_trades

{% endmacro %}