
{{ config(
        schema = 'collectionswap',
        alias = alias('trades'),
        tags = ['dunesql'],
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "collectionswap",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'collectionswap'
