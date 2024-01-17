{{
    config(
        schema = 'dfx_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS (
    SELECT 
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.trader AS taker,
        CAST(NULL as VARBINARY) as maker,
        t.originAmount as token_sold_amount_raw, 
        t.targetAmount as token_bought_amount_raw, 
        t.origin as token_sold_address, 
        t.target as token_bought_address, 
        t.contract_address as project_contract_address, 
        t.evt_tx_hash as tx_hash, 
        t.evt_index
    FROM {{ source('dfx_finance_ethereum', 'Curve_evt_Trade') }} t
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}
)

SELECT
    'ethereum' AS blockchain,
    'dfx' AS project,
    '0.5' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
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
