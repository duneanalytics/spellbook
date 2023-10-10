
{{ config(
	tags=['legacy'],
	
        schema = 'tofu',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism", "arbitrum", "polygon", "bnb"]\',
                                    "project",
                                    "tofu",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "tofu"
