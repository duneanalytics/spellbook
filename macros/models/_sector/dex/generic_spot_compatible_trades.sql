{% macro generic_spot_compatible_trades(
        blockchain = '',
        project = '',
        version = '',
        source_evt_swap = '',
        taker = 'account'
    )
%}

WITH dexs AS (
    SELECT
        t.evt_block_time as block_time,
        t.evt_block_number as block_number,
        t.{{ taker }} as taker,
        cast(null as varbinary) as maker,
        t.amountOut as token_bought_amount_raw,
        t.amountIn as token_sold_amount_raw,
        t.tokenOut as token_bought_address,
        t.tokenIn as token_sold_address,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    FROM {{ source_evt_swap }} t
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

{# ######################################################################### #}

{% macro generic_spot_v2_compatible_trades(
        blockchain = '',
        project = '',
        sources = [],
        maker = ''
    )
%}

WITH dexs AS (
    {% for src in sources %}
        SELECT
            '{{ src["version"] }}' as version,
            t.evt_block_time as block_time,
            t.evt_block_number as block_number,
            t.to as taker,
            {% if maker %}
                t.{{ maker }}
            {% else %}
                cast(null as varbinary)
            {% endif %} as maker,
            t.toAmount as token_bought_amount_raw,
            t.fromAmount as token_sold_amount_raw,
            t.toToken as token_bought_address,
            t.fromToken as token_sold_address,
            t.contract_address as project_contract_address,
            t.evt_tx_hash as tx_hash,
            t.evt_index
        FROM {{ source(project ~ '_' ~ blockchain, src["source"] )}} t
        WHERE 1 = 1
        {% if src["exclude"] %}
            AND t."from" NOT IN ({{ src["exclude"] }})
        {% endif %}
        {% if is_incremental() %}
            AND {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}{% if not loop.last %}
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
