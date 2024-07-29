{% macro airswap_compatible_trades(
    blockchain = null,
    project = null,
    sources = []
    )
%}

WITH dexs AS
(
    {% for src in sources %}
        SELECT
            '{{ src["version"] }}' as version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            senderWallet AS taker,
            signerWallet AS maker,
            senderAmount AS token_sold_amount_raw,
            signerAmount AS token_bought_amount_raw,
            senderToken AS token_sold_address,
            signerToken AS token_bought_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            evt_index
        FROM {{ source('airswap_' ~ blockchain, src["source"] )}}
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
