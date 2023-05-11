
{{ config(
        schema = 'opensea',
        alias ='trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "opensea"
