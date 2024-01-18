{{ config(
    schema = 'odos_optimism'
    ,alias = 'trades'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['tx_hash', 'evt_index']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH 
dexs as (
        SELECT 
            evt_block_time as block_time, 
            evt_block_number as block_number,
            json_extract_scalar(outputs[1], '$.receiver') AS taker,
            sender as maker,
            amountsIn[1] as token_sold_amount_raw, 
            amountsOut[1] as token_bought_amount_raw, 
            CAST(NULL as double) as amount_usd, 
            tokensIn[1] as token_sold_address,
            CAST(json_extract_scalar(outputs[1], '$.tokenAddress') AS varbinary) AS token_bought_address, 
            contract_address as project_contract_address, 
            evt_tx_hash as tx_hash, 
            evt_index,
            array[-1] as trace_address
        FROM 
        {{ source('odos_optimism', 'OdosRouter_evt_Swapped') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% endif %}
)

SELECT
    'optimism' AS blockchain,
    'odos' AS project,
    '1' AS version,
    CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    CAST(date_trunc('MONTH', dexs.block_time) AS date) AS block_month,
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
    dexs.evt_index,
    dexs.trace_address
FROM dexs
