{{ config(
    alias = 'trades'
    ,schema = 'odos_v2_base'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2023-07-13' %}

with event_data AS (
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        sender AS taker,
        contract_address AS maker,
        inputAmount AS token_sold_amount_raw,
        amountOut AS token_bought_amount_raw,
        CAST(NULL as double) as amount_usd,
        CASE
            WHEN inputToken = 0x0000000000000000000000000000000000000000
            THEN 0x4200000000000000000000000000000000000006 -- WETH
            ELSE inputToken
        END AS token_sold_address,
        CASE
            WHEN outputToken = 0x0000000000000000000000000000000000000000
            THEN 0x4200000000000000000000000000000000000006 -- WETH
            ELSE outputToken
        END AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index,
        CAST(ARRAY[-1] as array<bigint>) as trace_address
    FROM
    {{ source('odos_v2_base', 'OdosRouterV2_evt_Swap') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% else %}
        WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
)

SELECT
    'base' AS blockchain,
    'odos'     AS project,
    '2'        AS version,
    TRY_CAST(date_trunc('DAY', e.block_time) AS date)    AS block_date,
    TRY_CAST(date_trunc('MONTH', e.block_time) AS date)  AS block_month,
    e.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol)
        THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    e.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    e.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
    e.token_bought_amount_raw                              AS token_bought_amount_raw,
    e.token_sold_amount_raw                                AS token_sold_amount_raw,
    COALESCE(
        e.amount_usd,
        (e.token_bought_amount_raw / power(10, erc20a.decimals)) * p_bought.price,
        (e.token_sold_amount_raw / power(10, erc20b.decimals)) * p_sold.price
    ) AS amount_usd,
    CAST(e.token_bought_address AS varbinary) AS token_bought_address,
    e.token_sold_address,
    CAST(e.taker AS varbinary) AS taker,
    e.maker,
    e.project_contract_address,
    e.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    e.evt_index,
    e.trace_address
FROM event_data e
INNER JOIN {{ source('base', 'transactions') }} tx
    ON e.tx_hash = tx.hash
    {% if not is_incremental() %}
	AND tx.block_time >= DATE '{{project_start_date}}'
	{% else %}
	AND {{ incremental_predicate('tx.block_time') }}
	{% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = e.token_bought_address
    AND erc20a.blockchain = 'base'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = e.token_sold_address
    AND erc20b.blockchain = 'base'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', e.block_time)
    AND p_bought.contract_address = e.token_bought_address
    AND p_bought.blockchain = 'base'
    {% if not is_incremental() %}
	AND p_bought.minute >= DATE '{{project_start_date}}'
	{% else %}
	AND {{ incremental_predicate('p_bought.minute') }}
	{% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', e.block_time)
    AND p_sold.contract_address = e.token_sold_address
    AND p_sold.blockchain = 'base'
    {% if not is_incremental() %}
	AND p_sold.minute >= DATE '{{project_start_date}}'
	{% else %}
	AND {{ incremental_predicate('p_sold.minute') }}
	{% endif %}