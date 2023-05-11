
{{ config(
        schema = 'rarible',
        alias ='trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "rarible"
