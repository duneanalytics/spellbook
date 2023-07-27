
{{ config(
	tags=['legacy'],
	
        schema = 'quix',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "quix",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "quix"
