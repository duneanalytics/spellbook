{% macro generic_spot_compatible_trades(
        blockchain = '',
        project = '',
        version = '',
        source_evt_swap = ''
    )
%}

WITH dexs AS (
    SELECT
        t.evt_block_time as block_time,
        t.evt_block_number as block_number,
        t.account as taker,
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
