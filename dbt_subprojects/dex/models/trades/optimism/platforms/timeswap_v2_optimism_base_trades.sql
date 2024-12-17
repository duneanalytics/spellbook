{{
    config(
        schema = 'timeswap_v2_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , CAST(null AS VARBINARY) AS taker
        , t.contract_address AS maker
        , CASE WHEN t.token0AndLong0Amount < INT256 '0' THEN abs(t.token0AndLong0Amount) ELSE abs(t.token1AndLong1Amount) END AS token_bought_amount_raw
        , CASE WHEN t.token0AndLong0Amount < INT256 '0' THEN abs(t.token1AndLong1Amount) ELSE abs(t.token0AndLong0Amount) END AS token_sold_amount_raw
        , CASE WHEN t.token0AndLong0Amount < INT256 '0' THEN t.tokenTo ELSE t.longTo END AS token_bought_address
        , CASE WHEN t.token0AndLong0Amount < INT256 '0' THEN t.longTo ELSE t.tokenTo END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
        , t.evt_tx_from AS tx_from
        , t.evt_tx_to AS tx_to
    FROM {{ source('timeswap_v2_optimism', 'TimeswapV2Option_evt_Swap') }} t
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'optimism' AS blockchain
    , 'timeswap' AS project
    , '2' AS version
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
    , dexs.tx_from
    , dexs.tx_to
FROM
    dexs
