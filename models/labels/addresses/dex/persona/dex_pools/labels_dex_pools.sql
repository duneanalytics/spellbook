{{
    config(
        tags=['dunesql'],
        alias = alias('dex_pools'),
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "polygon"]\', 
                    "sector",
                    "labels",
                    \'["hildobby"]\') }}'
    )
}}

SELECT blockchain
, pool AS address
, 'DEX Pool' AS name
, 'dex' AS category
, 'hildobby' AS contributor
, 'query' AS source
, TIMESTAMP '2023-03-18' as created_at
, now() AS updated_at
, 'dex_pools' AS model_name
, 'persona' AS label_type
FROM {{ ref('dex_pools') }}