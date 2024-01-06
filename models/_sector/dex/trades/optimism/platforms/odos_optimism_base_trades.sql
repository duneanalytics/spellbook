{{ config(
    schema = 'odos_optimism'
    ,alias = 'base_trades'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['tx_hash', 'evt_index']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH 
dexs_raw as (
        SELECT 
            evt_block_time as block_time, 
            explode(outputs) as data_value, 
            '' as maker, 
            CAST(amountsIn[0] as double) as token_sold_amount_raw, 
            CAST(amountsOut[0] as double) as token_bought_amount_raw, 
            CAST(NULL as double) as amount_usd, 
            CAST(tokensIn[0] as string) as token_sold_address, 
            contract_address as project_contract_address, 
            evt_tx_hash as tx_hash, 
            CAST(ARRAY() as array<bigint>) AS trace_address,
            evt_index
        FROM 
        {{ source('odos_optimism', 'OdosRouter_evt_Swapped') }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% endif %}
), 

dexs as (
        SELECT 
            *, 
            CAST(data_value:receiver as string) as taker, 
            CAST(data_value:tokenAddress as string) as token_bought_address
        FROM 
        dexs_raw
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
    dexs.evt_index
FROM dexs
