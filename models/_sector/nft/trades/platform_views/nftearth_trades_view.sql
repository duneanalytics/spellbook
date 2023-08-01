
{{ config(
        schema = 'nftearth',
        alias = alias('trades'),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "nftearth",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "nftearth"
