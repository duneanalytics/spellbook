{% macro woofi_compatible_trades(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , pair_column_name = 'pair'
    )
%}

WITH dexs AS
(
        SELECT
            t.evt_block_number AS block_number,
            t.evt_block_time AS block_time,
            t."from" AS taker,
            t.to AS maker,
            t.toAmount AS token_sold_amount_raw,
            t.fromAmount AS token_bought_amount_raw,
            t.toToken AS token_sold_address,
            t.fromToken AS token_bought_address,
            t.contract_address AS project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index
        FROM {{ Pair_evt_Swap}} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
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
