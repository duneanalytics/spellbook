{% macro openocean_compatible_v2_trades(
    blockchain = null
    , evt_swapped = null
    , burn_addresses = []
    , w_native = null
    )
%}

WITH dexs AS (
    SELECT
        t.evt_block_time AS block_time,
        CAST(date_trunc('day', t.evt_block_time) AS date) AS block_date,
        t.evt_block_number AS block_number,
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

, dexs_with_tx AS (
    {{
        add_tx_columns(
            model_cte = 'dexs'
            , blockchain = blockchain
            , columns = ['from', 'to']
        )
    }}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    'openocean' AS project,
    '2' AS version,
    block_date,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    block_time,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    tx_from,
    tx_to,
    trace_address,
    evt_index
FROM dexs_with_tx

{% endmacro %}
