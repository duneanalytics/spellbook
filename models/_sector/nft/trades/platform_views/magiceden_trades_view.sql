
{{ config(
        schema = 'magiceden',
        alias = 'trades',
        
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana", "polygon"]\',
                                    "project",
                                    "magiceden",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'magiceden'
