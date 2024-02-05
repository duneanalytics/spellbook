{{ config(
    schema = 'odos_v1_optimism'
    ,alias = 'trades'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2022-10-06' %}

WITH 
dexs as (
        SELECT 
            evt_block_time as block_time, 
            evt_block_number as block_number,
            json_extract_scalar(outputs[1], '$.receiver') AS taker,
            CAST(NULL as VARBINARY) as maker,
            amountsIn[1] as token_sold_amount_raw, 
            amountsOut[1] as token_bought_amount_raw, 
            CAST(NULL as double) as amount_usd, 
            tokensIn[1] as token_sold_address,
            json_extract_scalar(outputs[1], '$.tokenAddress') AS token_bought_address, 
            contract_address as project_contract_address, 
            evt_tx_hash as tx_hash, 
            evt_index,
            array[-1] as trace_address
        FROM 
        {{ source('odos_optimism', 'OdosRouter_evt_Swapped') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% else %}
        WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
)

SELECT
    'optimism'                                             AS blockchain,
    'odos'                                     AS project,
    '1'                                                    AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date)    AS block_date,
    TRY_CAST(date_trunc('MONTH', dexs.block_time) AS date)  AS block_month,
    dexs.block_time,
    erc20a.symbol                                          AS token_bought_symbol,
    erc20b.symbol                                          AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol)
        THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END                                                  AS token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
    dexs.token_bought_amount_raw                              AS token_bought_amount_raw,
    dexs.token_sold_amount_raw                               AS token_sold_amount_raw,
    COALESCE(
        dexs.amount_usd,
        (dexs.token_bought_amount_raw / power(10, erc20a.decimals)) * p_bought.price,
        (dexs.token_sold_amount_raw / power(10, erc20b.decimals)) * p_sold.price
    )                                                     AS amount_usd,
    CAST(dexs.token_bought_address AS varbinary) AS token_bought_address,
    dexs.token_sold_address,
    CAST(dexs.taker AS varbinary)                     AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from"                                              AS tx_from,
    tx.to                                                  AS tx_to,
    dexs.evt_index,
    dexs.trace_address
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
	AND tx.block_time >= DATE '{{project_start_date}}'
	{% else %}
	AND {{ incremental_predicate('tx.block_time') }}
	{% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON CAST(erc20a.contract_address AS varchar) = dexs.token_bought_address
    AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND CAST(p_bought.contract_address AS varchar) = dexs.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
	AND p_bought.minute >= DATE '{{project_start_date}}'
	{% else %}
	AND {{ incremental_predicate('p_bought.minute') }}
	{% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
	AND p_sold.minute >= DATE '{{project_start_date}}'
	{% else %}
	AND {{ incremental_predicate('p_sold.minute') }}
	{% endif %}
