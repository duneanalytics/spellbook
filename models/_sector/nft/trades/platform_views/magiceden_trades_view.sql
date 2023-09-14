
{{ config(
        schema = 'magiceden',
        alias = alias('trades'),
        tags = ['dunesql'],
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana", "polygon"]\',
                                    "project",
                                    "magiceden",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'magiceden'
