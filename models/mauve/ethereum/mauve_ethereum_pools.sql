{{ config(
    tags = ['dunesql'],
    schema = 'mauve_ethereum',
    alias = alias('pools'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "mauve",
                                \'["hildobby","raphaelr"]\') }}'
    )
}}

SELECT 'ethereum' AS blockchain
, 'mauve' AS project
, '1' AS version
, pool
, fee
, token0
, token1
, evt_block_time AS creation_block_time
, evt_block_number AS creation_block_number
, contract_address
FROM {{ source('mauve_ethereum', 'MauveFactory_evt_PoolCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}