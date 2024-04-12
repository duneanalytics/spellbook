{{ config(
    
    schema = 'equalizer_fantom',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "equalizer",
                                \'["Henrystats"]\') }}'
    )
}}

SELECT 'fantom' AS blockchain
, 'equalizer' AS project
, '1' AS version
, pair AS pool
, CASE WHEN stable = true THEN CAST(0.02 as decimal) ELSE CAST(0.2 as decimal) END as fee 
, token0
, token1
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('equalizer_exchange_fantom', 'PairFactory_evt_PairCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
{% endif %}