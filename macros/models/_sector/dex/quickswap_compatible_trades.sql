{% macro quickswap_compatible_trades(
    blockchain = null,
    project = null,
    version = null,
    sources = []
    )
%}


WITH dexs AS
(
    --QuickSwap v3
    SELECT
        t.evt_block_number AS block_number,
        ,t.evt_block_time AS block_time
        ,t.recipient AS taker
        ,CAST(NULL AS VARBINARY) AS maker
        ,CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        ,CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        ,CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        ,CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
        ,t.contract_address as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,t.evt_index
    FROM
        {{ source('quickswap_v3_polygon', 'AlgebraPool_evt_Swap') }} t
    INNER JOIN 
        {{ source('quickswap_v3_polygon', 'AlgebraFactory_evt_Pool') }} f
        ON f.pool = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
SELECT
    '{{ blockchain }}' AS blockchain
    ,'{{ project }}' AS project
    ,'{{ version }}' AS version
    dexs.version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs

{% endmacro %}
