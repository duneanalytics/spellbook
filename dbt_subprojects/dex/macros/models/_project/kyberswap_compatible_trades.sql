{% macro kyberswap_compatible_trades(
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
            t.evt_block_number AS block_number,
            t.evt_block_time AS block_time,
            t.sender AS taker,
            t.recipient AS maker,
            cast(if(starts_with(cast(t.deltaQty0 as varchar), '-'), abs(t.deltaQty1), abs(t.deltaQty0)) as uint256) AS token_bought_amount_raw,
            cast(if(starts_with(cast(t.deltaQty0 as varchar), '-'), abs(t.deltaQty0), abs(t.deltaQty1)) as uint256) AS token_sold_amount_raw,
            if(starts_with(cast(t.deltaQty0 as varchar), '-'), p.token1, p.token0) AS token_bought_address,
            if(starts_with(cast(t.deltaQty0 as varchar), '-'), p.token0, p.token1) AS token_sold_address,
            t.contract_address AS project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index
        FROM {{ source('kyber_' ~ blockchain, src["source_evt_swap"]) }} t
            INNER JOIN {{ source('kyber_' ~ blockchain, src["source_evt_factory"]) }} p ON t.contract_address = p.pool
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.evt_block_time') }}
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
