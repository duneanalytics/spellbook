{{ config(
	tags=['legacy'],
	
    schema = 'openocean_v2_fantom',
    alias = alias('trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "openocean_v2",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2021-09-18' %}

WITH 

dexs as (
    SELECT 
        evt_block_time as block_time, 
        dstReceiver as taker, 
        '' as maker, 
        returnAmount as token_bought_amount_raw, 
        spentAmount as token_sold_amount_raw, 
        CAST(NULL as double) as amount_usd, 
        CASE 
            WHEN CAST(dstToken as string) IN ('0', 'O', '0x0000000000000000000000000000000000000000')
            THEN '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' -- wftm
            ELSE CAST(dstToken as string)
        END as token_bought_address,  
        CASE 
            WHEN CAST(srcToken as string) IN ('0', 'O', '0x0000000000000000000000000000000000000000')
            THEN '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' -- wftm
            ELSE CAST(srcToken as string)
        END as token_sold_address,
        contract_address as project_contract_address,
        evt_tx_hash as tx_hash, 
        CAST(ARRAY() as array<bigint>) AS trace_address,
        evt_index
    FROM 
    {{ source('open_ocean_fantom', 'OpenOceanExchange_evt_Swapped') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    'fantom' as blockchain, 
    'openocean' as project, 
    '2' as version, 
    TRY_CAST(date_trunc('DAY', dexs.block_time) as date) as block_date, 
    dexs.block_time, 
    erc20a.symbol as token_bought_symbol, 
    erc20b.symbol as token_sold_symbol, 
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END as token_pair, 
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount, 
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount, 
    CAST(dexs.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw, 
    CAST(dexs.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw, 
    COALESCE(
        dexs.amount_usd, 
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price, 
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) as amount_usd, 
    dexs.token_bought_address, 
    dexs.token_sold_address, 
    COALESCE(dexs.taker, tx.from) as taker,  -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker, 
    dexs.project_contract_address, 
    dexs.tx_hash, 
    tx.from as tx_from, 
    tx.to AS tx_to, 
    dexs.trace_address, 
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('fantom', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20_legacy') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'fantom'
LEFT JOIN {{ ref('tokens_erc20_legacy') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'fantom'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'fantom'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'fantom'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
;