{{ config(
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "spookyswap",
                                \'["Henrystats"]\') }}'
    )
}}

SELECT 'fantom' AS blockchain
, 'spookswap' AS project
, '1' AS version
, pair AS pool
, 0.2 AS fee
, token0
, token1
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('spookyswap_fantom', 'UniswapV2Factory_evt_PairCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}