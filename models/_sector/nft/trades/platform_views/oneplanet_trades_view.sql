
{{ config(
	tags=['legacy'],
	
        schema = 'oneplanet',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "oneplanet",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "oneplanet"
