
{{ config(
	tags=['legacy'],
	
        schema = 'magiceden',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana", "polygon"]\',
                                    "project",
                                    "magiceden",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "magiceden"
