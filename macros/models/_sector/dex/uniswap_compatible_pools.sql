{% macro uniswap_compatible_v2_pools(
    blockchain = null
    , project = null
    , version = null
    , Factory_evt_PairCreated = null
    )
%}

WITH pools AS
(
    SELECT
        evt_block_time AS block_time
        , evt_block_number AS block_number
        , pair AS pool
        , 0.3 AS fee
        , token0
        , token1
        , contract_address
        , evt_tx_hash AS tx_hash
        , evt_index
    FROM
        {{ Factory_evt_PairCreated }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , pool
    , fee
    , token0
    , token1
    , contract_address
    , tx_hash
    , evt_index
FROM
    pools
{% endmacro %}