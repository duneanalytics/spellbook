
{{ config(
	tags=['legacy'],
	
        schema = 'nftearth',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "nftearth",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "nftearth"
