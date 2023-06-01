
{{ config(
        schema = 'aavegotchi',
        alias ='trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "aavegotchi",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "aavegotchi"
