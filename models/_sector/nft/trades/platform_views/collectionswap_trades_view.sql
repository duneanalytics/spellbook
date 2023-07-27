
{{ config(
	tags=['legacy'],
	
        schema = 'collectionswap',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "collectionswap",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "collectionswap"
