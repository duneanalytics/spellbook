
{{ config(
	tags=['legacy'],
	
        schema = 'pancakeswap_nft',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "pancakeswap_nft",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "pancakeswap"
