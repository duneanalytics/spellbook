{% macro dodo_compatible_trades(
    blockchain = '',
    project = '',
    markets = '',
    decoded_project = 'dodo',
    sell_base_token_source = '',
    buy_base_token_source = '',
    other_sources = []
    )
%}

WITH

{% if markets != '' %}
markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS (
    {{ markets }}
),
{% endif %}

{% if sell_base_token_source != '' or buy_base_token_source != '' %}
base_token_dexs AS (
    {% if sell_base_token_source != '' %}
        -- dodo v1 sell
        SELECT
            '1' AS version,
            s.evt_block_number AS block_number,
            s.evt_block_time AS block_time,
            s.seller AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            s.payBase AS token_bought_amount_raw,
            s.receiveQuote AS token_sold_amount_raw,
            m.base_token_address AS token_bought_address,
            m.quote_token_address AS token_sold_address,
            s.contract_address AS project_contract_address,
            s.evt_tx_hash AS tx_hash,
            s.evt_index
        FROM {{ source(decoded_project ~ '_' ~ blockchain, sell_base_token_source )}} s
        LEFT JOIN markets m ON s.contract_address = m.market_contract_address
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('s.evt_block_time') }}
        {% endif %}
    {% endif %}

    {% if sell_base_token_source != '' and buy_base_token_source != '' %}
        UNION ALL
    {% endif %}

    {% if buy_base_token_source != '' %}
        -- dodo v1 buy
        SELECT
            '1' AS version,
            b.evt_block_number AS block_number,
            b.evt_block_time AS block_time,
            b.buyer AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            b.receiveBase AS token_bought_amount_raw,
            b.payQuote AS token_sold_amount_raw,
            m.base_token_address AS token_bought_address,
            m.quote_token_address AS token_sold_address,
            b.contract_address AS project_contract_address,
            b.evt_tx_hash AS tx_hash,
            b.evt_index
        FROM {{ source(decoded_project ~ '_' ~ blockchain, buy_base_token_source )}} b
        LEFT JOIN markets m ON b.contract_address = m.market_contract_address
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('b.evt_block_time') }}
        {% endif %}
    {% endif %}
),
{% endif %}

other_dexs AS (
    {% for src in other_sources %}
        SELECT
            '{{ src["version"] }}' as version,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            evt_index
        FROM {{ source(decoded_project ~ '_' ~ blockchain, src["source"] )}}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),

dexs AS (
    {% if sell_base_token_source != '' or buy_base_token_source != '' %}
    SELECT * FROM base_token_dexs
    UNION ALL
    {% endif %}
    SELECT * FROM other_dexs
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
