{%- macro yield_yak_base_trades(
        blockchain = null,
        project_start_date = '2021-09-15'
    )
-%}

{%- set namespace_blockchain = 'yield_yak_' + blockchain -%}

WITH dexs AS (
    SELECT 
        evt_block_time AS block_time
        -- , '' AS taker commenting this as there's no trader in the event
        , CAST(NULL as VARBINARY) AS maker
        , _amountIn AS token_sold_amount_raw
        , _amountOut AS token_bought_amount_raw
        , CAST(NULL AS double) AS amount_usd
        , _tokenIn AS token_sold_address
        , _tokenOut AS token_bought_address
        , contract_address As project_contract_address
        , evt_tx_hash AS tx_hash
        , ARRAY[-1] AS trace_address
        , evt_index
    FROM {{ source(namespace_blockchain, 'YakRouter_evt_YakSwap') }}
    {%- if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , 'yield_yak' AS project
    , '1' AS version
    , CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    , CAST(date_trunc('MONTH', dexs.block_time) AS date) AS block_month
    , dexs.block_time
    , dexs.token_bought_amount_raw AS token_bought_amount_raw
    , dexs.token_sold_amount_raw AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , tx."from" AS taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , tx."from" AS tx_from
    , tx.to AS tx_to
    , dexs.trace_address
    , dexs.evt_index
FROM dexs
INNER JOIN {{ source(blockchain, 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {%- if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{ project_start_date }}'
    {%- endif %}
    {%- if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {%- endif %}

{%- endmacro -%}
