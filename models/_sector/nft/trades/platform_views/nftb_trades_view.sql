
{{ config(
        schema = 'nftb',
        alias = alias('trades'),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "nftb",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "nftb"
