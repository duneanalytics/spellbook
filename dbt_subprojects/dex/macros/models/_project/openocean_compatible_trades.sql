{% macro openocean_compatible_v2_trades(
    blockchain = null
    , evt_swapped = null
    , burn_addresses = []
    , w_native = null
    , project_start_date = null
    )
%}

WITH dexs AS (
    SELECT
        t.evt_block_time AS block_time,
        t.dstReceiver AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        t.returnAmount AS token_bought_amount_raw,
        t.spentAmount AS token_sold_amount_raw,
        CASE WHEN t.dstToken IN ({{ burn_addresses | join(', ') }}) THEN {{ w_native }} ELSE t.dstToken END AS token_bought_address,
        CASE WHEN t.srcToken IN ({{ burn_addresses | join(', ') }}) THEN {{ w_native }} ELSE t.srcToken END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        ARRAY[-1] AS trace_address,
        t.evt_index
    FROM {{ evt_swapped }} t
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    'openocean' AS project,
    '2' AS version,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    dexs.block_time,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source(blockchain, 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% endif %}

{% endmacro %}
