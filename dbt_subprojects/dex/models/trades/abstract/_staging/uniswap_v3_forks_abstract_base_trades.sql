{% set blockchain = 'abstract' %}

{{ config(
        schema = 'uniswap_v3_forks_' + blockchain,
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

WITH evt_swap AS (
    SELECT
        blockchain
        , block_number
        , block_time
        , recipient
        , sender
        , amount0
        , amount1
        , contract_address
        , pool_topic0
        , tx_hash
        , tx_index
        , evt_index
        , tx_from
        , tx_to
    FROM {{ ref('uniswap_v3_forks_' + blockchain + '_decoded_swap_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)
-- filtering out bogus factory deployments
, factory_event_counts AS (
    SELECT 
        contract_address
        , pool
        , blockchain
        , COUNT(*) as event_count
    FROM {{ ref('uniswap_v3_forks_' + blockchain + '_decoded_factory_events') }}
    GROUP BY pool, blockchain, contract_address
    HAVING COUNT(*) = 1 -- Only keep pools with exactly one factory event
)
, latest_creation_traces AS (
    SELECT 
        *
    FROM
        {{ ref('abstract_latest_creation_trace') }}
)
, dexs AS (
    SELECT
        t.blockchain
        , t.block_number
        , t.block_time
        , t.recipient AS taker
        , t.sender as maker
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
        {{ ref('uniswap_v3_forks_' + blockchain + '_decoded_factory_events') }} f
        ON f.pool = t.contract_address
        AND f.blockchain = t.blockchain
    INNER JOIN
        factory_event_counts fec
        ON fec.pool = f.pool
        AND fec.blockchain = f.blockchain
        AND fec.contract_address = f.contract_address
    INNER JOIN latest_creation_traces ct
        ON ct.address = f.pool
        AND ct."from" = f.contract_address
        AND ct.blockchain = f.blockchain
        AND ct.block_month = f.block_month
)

SELECT
    blockchain
    , '3' AS version
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
FROM dexs