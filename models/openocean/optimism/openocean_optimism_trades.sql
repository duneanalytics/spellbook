{{ config(
    schema = 'openocean_optimism',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "openocean",
                                \'["ARDev097"]\') }}'
    )
}}

{% set project_start_date = '2022-01-20' %}

WITH 
dexs as (
    SELECT 
        evt_block_time as block_time, 
        dstReceiver as taker, 
        CAST(NULL as VARBINARY) as maker,
        returnAmount as token_bought_amount_raw, 
        spentAmount as token_sold_amount_raw, 
        CAST(NULL as double) as amount_usd, 
        dstToken as token_bought_address,  
        srcToken as token_sold_address,
        contract_address as project_contract_address,
        evt_tx_hash as tx_hash, 
        ARRAY[-1] AS trace_address,
        evt_index
    FROM {{ source('openocean_v2_optimism', 'OpenOceanExchangeProxy_evt_Swapped') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
SELECT
    'optimism' as blockchain, 
    'openocean' as project, 
    '2' as version, 
    cast(date_trunc('DAY', dexs.block_time) as date) as block_date, 
    cast(date_trunc('month', dexs.block_time) as date) as block_month, 
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
    COALESCE(dexs.taker, tx."from") as taker,  
    dexs.maker, 
    dexs.project_contract_address, 
    dexs.tx_hash, 
    tx."from" as tx_from, 
    tx.to AS tx_to,
    trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= timestamp '{{project_start_date}}'
    {% else %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_bought.minute >= timestamp '{{project_start_date}}'
    {% else %}
    AND {{ incremental_predicate('p_bought.minute') }}
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_sold.minute >= timestamp '{{project_start_date}}'
    {% else %}
    AND {{ incremental_predicate('p_sold.minute') }}
    {% endif %}