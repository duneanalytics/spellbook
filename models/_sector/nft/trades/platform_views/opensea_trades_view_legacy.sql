
{{ config(
	tags=['legacy'],
	
        schema = 'opensea',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "polygon"]\',
                                    "project",
                                    "opensea",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "opensea"
