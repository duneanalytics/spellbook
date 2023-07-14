
{{ config(
	tags=['legacy'],
	
        schema = 'nftrade',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "nftrade",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "nftrade"
