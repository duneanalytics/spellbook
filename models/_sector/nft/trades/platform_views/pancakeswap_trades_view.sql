
{{ config(
        schema = 'pancakeswap_nft',
        alias = alias('trades'),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "pancakeswap_nft",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "pancakeswap"
