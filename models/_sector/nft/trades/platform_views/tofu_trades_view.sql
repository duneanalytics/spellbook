
{{ config(
        schema = 'tofu',
        alias ='trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism", "arbitrum", "polygon", "bnb"]\',
                                    "sector",
                                    "nft",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "tofu"
