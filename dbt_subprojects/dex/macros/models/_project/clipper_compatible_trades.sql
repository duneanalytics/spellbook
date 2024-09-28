{% macro clipper_compatible_trades(
    blockchain = null,
    project = null,
    sources = []
    )
%}

WITH dexs AS (
    {% for src in sources %}
        SELECT
            '{{ src["version"] }}' as version,
            t.evt_block_number AS block_number,
            t.evt_block_time AS block_time,
            t.recipient as taker,
            CAST(NULL AS VARBINARY) AS maker,
            t.inAmount as token_sold_amount_raw,
            t.outAmount as token_bought_amount_raw,
            t.inAsset as token_sold_address,
            t.outAsset as token_bought_address,
            t.contract_address AS project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index
        FROM {{ source('clipper_' ~ blockchain, src["source"] )}} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
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
