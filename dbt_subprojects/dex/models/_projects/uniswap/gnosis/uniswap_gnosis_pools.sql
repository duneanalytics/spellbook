{{ config(
    schema = 'uniswap_gnosis',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "uniswap",
                                \'["hildobby"]\') }}'
) }}

SELECT 
    'gnosis' AS blockchain,
    'uniswap' AS project,
    'v3' AS version, -- assuming gnosis only has v3 with dynamic fees
    pool AS pool,
    fee,
    token0,
    token1,
    evt_block_time AS creation_block_time,
    evt_block_number AS creation_block_number,
    contract_address
FROM {{ source('uniswap_v3_gnosis', 'uniswapv3factory_evt_poolcreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}