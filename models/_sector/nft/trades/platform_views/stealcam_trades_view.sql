
{{ config(
        schema = 'stealcam',
        alias ='trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "nft",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "stealcam"
