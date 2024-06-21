{%- macro yield_yak_trades(
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

, basic_trades AS (
    SELECT
        '{{ blockchain }}' AS blockchain
        , 'yield_yak' AS project
        , '1' AS version
        , CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
        , CAST(date_trunc('MONTH', dexs.block_time) AS date) AS block_month
        , dexs.block_time
        , erc20a.symbol AS token_bought_symbol
        , erc20b.symbol AS token_sold_symbol
        , CASE
            WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
            ELSE concat(erc20a.symbol, '-', erc20b.symbol)
            END AS token_pair
        , dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
        , dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
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
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
        ON erc20a.contract_address = dexs.token_bought_address
        AND erc20a.blockchain = '{{ blockchain }}'
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
        ON erc20b.contract_address = dexs.token_sold_address
        AND erc20b.blockchain = '{{ blockchain }}'
)

, enrichments_with_prices AS (
    {{
        add_amount_usd(
            trades_cte = 'basic_trades'
        )
    }}
)

SELECT blockchain,
       project,
       version,
       block_month,
       block_date,
       block_time,
       token_bought_symbol,
       token_sold_symbol,
       token_pair,
       token_bought_amount,
       token_sold_amount,
       token_bought_amount_raw,
       token_sold_amount_raw,
       amount_usd,
       token_bought_address,
       token_sold_address,
       taker,
       maker,
       project_contract_address,
       tx_hash,
       tx_from,
       tx_to,
       trace_address,
       evt_index
FROM enrichments_with_prices
{%- endmacro -%}
