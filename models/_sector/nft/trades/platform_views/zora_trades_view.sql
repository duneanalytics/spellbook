
{{ config(
        schema = 'zora',
        alias = alias('trades'),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "zora",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "zora"
