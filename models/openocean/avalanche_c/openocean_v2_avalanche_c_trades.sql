{{ config(
    
    schema = 'openocean_v2_avalanche_c',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2021-09-09' %}

WITH 

dexs as (
    SELECT 
        evt_block_time as block_time, 
        dstReceiver as taker, 
        CAST(NULL as VARBINARY) as maker,
        returnAmount as token_bought_amount_raw, 
        spentAmount as token_sold_amount_raw, 
        CAST(NULL as double) as amount_usd, 
        CASE 
            WHEN dstToken = 0x0000000000000000000000000000000000000000
            THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- wavax 
            ELSE dstToken
        END as token_bought_address,  
        CASE 
            WHEN srcToken = 0x0000000000000000000000000000000000000000
            THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- wavax 
            ELSE srcToken
        END as token_sold_address,
        contract_address as project_contract_address,
        evt_tx_hash as tx_hash, 
        ARRAY[-1] AS trace_address,
        evt_index
    FROM {{ source('openocean_v2_avalanche_c', 'OpenOceanExchange_evt_Swapped') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT
    'avalanche_c' as blockchain, 
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
    COALESCE(dexs.taker, tx."from") as taker,  -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker, 
    dexs.project_contract_address, 
    dexs.tx_hash, 
    tx."from" as tx_from, 
    tx.to AS tx_to,
    trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('avalanche_c', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= timestamp '{{project_start_date}}'
    {% else %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'avalanche_c'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'avalanche_c'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    AND p_bought.minute >= timestamp '{{project_start_date}}'
    {% else %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    AND p_sold.minute >= timestamp '{{project_start_date}}'
    {% else %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}