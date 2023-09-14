
{{ config(
        schema = 'tofu',
        alias = alias('trades'),
        tags = ['dunesql'],
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism", "arbitrum", "polygon", "bnb"]\',
                                    "project",
                                    "tofu",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'tofu'
