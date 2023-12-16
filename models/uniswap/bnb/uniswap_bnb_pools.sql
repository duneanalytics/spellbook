{{ config(
    
    schema = 'uniswap_bnb',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "uniswap",
                                \'["mtitus6", "hildobby"]\') }}'
    )
}}

SELECT 'bnb' AS blockchain
, 'uniswap' AS project
, 'v3' AS version
, pool
, fee
, array_agg(ROW(token0, token1)) AS tokens
, 2 AS tokens_in_pool
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('uniswap_v3_bnb', 'Factory_evt_PoolCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 10