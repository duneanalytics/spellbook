{{ config(
    schema = 'yield_yak_arbitrum',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


{% set project_start_date = '2023-03-21' %}

WITH 

dexs as (
        SELECT 
            evt_block_time as block_time, 
            -- '' as taker, commenting this as there's no trader in the event 
            CAST(NULL as VARBINARY) as maker, 
            _amountIn as token_sold_amount_raw, 
            _amountOut as token_bought_amount_raw, 
            CAST(NULL as double) as amount_usd, 
            _tokenIn as token_sold_address, 
            _tokenOut as token_bought_address, 
            contract_address as project_contract_address, 
            evt_tx_hash as tx_hash,
            ARRAY[-1] AS trace_address,
            evt_index
        FROM 
        {{ source('yield_yak_arbitrum', 'YakRouter_evt_YakSwap') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% endif %}
)

SELECT
    'arbitrum' as blockchain, 
    'yield_yak' as project, 
    '1' as version, 
    CAST(date_trunc('DAY', dexs.block_time) as date) as block_date, 
    CAST(date_trunc('MONTH', dexs.block_time) as date) as block_month, 
    dexs.block_time, 
    erc20a.symbol as token_bought_symbol, 
    erc20b.symbol as token_sold_symbol, 
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END as token_pair, 
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount, 
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount, 
    dexs.token_bought_amount_raw AS token_bought_amount_raw, 
    dexs.token_sold_amount_raw AS token_sold_amount_raw, 
    COALESCE(
        dexs.amount_usd, 
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price, 
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) as amount_usd, 
    dexs.token_bought_address, 
    dexs.token_sold_address, 
    tx."from" as taker,
    dexs.maker, 
    dexs.project_contract_address, 
    dexs.tx_hash, 
    tx."from" as tx_from, 
    tx.to AS tx_to, 
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{incremental_predicate('p_bought.minute')}}
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% endif %}