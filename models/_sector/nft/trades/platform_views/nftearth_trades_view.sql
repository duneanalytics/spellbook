
{{ config(
        schema = 'nftearth',
        alias ='trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "nft",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "nftearth"
