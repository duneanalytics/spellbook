{{ config(
    schema = 'uniswap_v3_ethereum',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

SELECT 
    'ethereum' AS blockchain
    , 'uniswap' AS project
    , '2' AS version
    , pool
    , 0 as fee -- testing to see if this is the issue
    , array_agg(
        ROW(f.token0, f.token1)
    ) AS tokens
    , 2 AS tokens_in_pool
    , evt_block_time AS creation_block_time
    , evt_block_number AS creation_block_number
    , contract_address
FROM {{source('uniswap_v2_ethereum', 'Factory_evt_PairCreated')}}
{% if is_incremental() %}
WHERE
{{ incremental_predicate('f.evt_block_time') }} f 
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 10