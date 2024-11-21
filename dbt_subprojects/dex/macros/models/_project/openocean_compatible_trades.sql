{% macro openocean_compatible_v2_trades(
    blockchain = null
    , project = null
    , version = null
    , evt_swapped = null
    , burn = null
    , w_native = null
    )
%}

WITH dexs AS (
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.dstReceiver as taker,
        CAST(NULL AS VARBINARY) AS maker,
        t.returnAmount as token_bought_amount_raw,
        t.spentAmount as token_sold_amount_raw,
        CASE WHEN t.dstToken = {{ burn }} THEN {{ w_native }} ELSE t.dstToken END as token_bought_address,  
        CASE WHEN t.srcToken = {{ burn }} THEN {{ w_native }} ELSE t.srcToken END as token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM {{ evt_swapped }} t
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
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