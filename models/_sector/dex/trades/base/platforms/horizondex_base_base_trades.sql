{{
    config(
    schema = 'horizondex_base'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.recipient AS taker
        , cast(null as varbinary) as maker
        , CASE WHEN t.deltaQty0 < INT256 '0' THEN abs(t.deltaQty0) ELSE abs(t.deltaQty1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        , CASE WHEN t.deltaQty0 < INT256 '0' THEN abs(t.deltaQty1) ELSE abs(t.deltaQty0) END AS token_sold_amount_raw
        , CASE WHEN t.deltaQty0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
        , CASE WHEN t.deltaQty0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address as project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
    FROM
        {{ source('horizondex_base', 'Pool_evt_Swap') }} t
    INNER JOIN
        {{ source('horizondex_base', 'Factory_evt_PoolCreated') }} f
        ON f.pool = t.contract_address
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'base' AS blockchain
    ,'horizondex' AS project
    ,'1' AS version
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
FROM
    dexs