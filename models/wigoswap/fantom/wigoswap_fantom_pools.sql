{{ config(
    
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "wigoswap",
                                \'["Henrystats"]\') }}'
    )
}}

SELECT 'fantom' AS blockchain
, 'wigoswap' AS project
, '1' AS version
, pair AS pool
, CAST(0.19 as decimal) AS fee
, array_agg(ROW(token0, token1)) AS tokens
, 2 AS tokens_in_pool
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('wigoswap_fantom', 'Factory_evt_PairCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
{% endif %}