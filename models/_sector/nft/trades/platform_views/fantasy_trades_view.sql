
{{ config(
        schema = 'fantasy',
        alias = 'trades',
        
        materialized = 'view',
        post_hook='{{ expose_spells(\'["blast"]\',
                                    "project",
                                    "nftrade",
                                    \'["hildobby"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'fantasy'
