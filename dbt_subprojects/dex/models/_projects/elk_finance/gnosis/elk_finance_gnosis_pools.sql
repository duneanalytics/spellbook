{{ config(
    schema = 'elk_finance_gnosis',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool']
) }}

SELECT 
    'gnosis' AS blockchain,
    'elk_finance' AS project,
    'v2' AS version,
    pair AS pool,
    3000 AS fee,
    token0,
    token1,
    evt_block_time AS creation_block_time,
    evt_block_number AS creation_block_number,
    contract_address
FROM {{ source('elk_finance_gnosis', 'ElkFactory_evt_PairCreated') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}