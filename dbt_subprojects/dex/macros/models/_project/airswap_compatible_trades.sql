{% macro airswap_compatible_trades(
    blockchain = null,
    project = null,
    sources = []
    )
%}

{% set default_cols = {
    'taker': 'senderWallet',
    'maker': 'signerWallet',
    'token_sold_amount_raw': 'senderAmount',
    'token_bought_amount_raw': 'signerAmount',
    'token_sold_address': 'senderToken',
    'token_bought_address': 'signerToken'
} %}

WITH dexs AS
(
    {% for src in sources %}
        {% set cols = default_cols.copy() %}
        {% if src.get("cols") %}
            {% set _ = cols.update(src["cols"]) %}
        {% endif %}
        SELECT
            '{{ src["version"] }}' as version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            {{ cols["taker"] }} AS taker,
            {{ cols["maker"] }} AS maker,
            {{ cols["token_sold_amount_raw"] }} AS token_sold_amount_raw,
            {{ cols["token_bought_amount_raw"] }} AS token_bought_amount_raw,
            {{ cols["token_sold_address"] }} AS token_sold_address,
            {{ cols["token_bought_address"] }} AS token_bought_address,
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
