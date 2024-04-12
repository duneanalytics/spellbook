{% macro maverick_compatible_trades(
        blockchain = '',
        project = '',
        version = '',
        source_evt_swap = '',
        source_evt_pool = ''
    )
%}

WITH dexs AS (
    SELECT
        t.evt_block_time AS block_time,
        t.evt_block_number AS block_number,
        t.recipient AS taker,
        CAST(NULL AS varbinary) AS maker,
        t.amountOut AS token_bought_amount_raw,
        t.amountIn AS token_sold_amount_raw,
        CASE WHEN t.tokenAIn THEN f.tokenB ELSE f.tokenA END AS token_bought_address,
        CASE WHEN t.tokenAIn THEN f.tokenA ELSE f.tokenB END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM {{ source_evt_swap }} t
        INNER JOIN {{ source_evt_pool }} f ON t.contract_address = f.poolAddress
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
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
