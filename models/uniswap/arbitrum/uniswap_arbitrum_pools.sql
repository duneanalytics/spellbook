{{ config(
    schema = 'uniswap_arbitrum',
    alias = alias('pools'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "uniswap",
                                \'["hildobby"]\') }}'
    )
}}

SELECT 'arbitrum' AS blockchain
, 'uniswap' AS project
, 'v3' AS version
, pool
, fee
, token0
, token1
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('uniswap_v3_arbitrum', 'Factory_evt_PoolCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}