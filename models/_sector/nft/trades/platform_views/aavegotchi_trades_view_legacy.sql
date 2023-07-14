
{{ config(
	tags=['legacy'],
	
        schema = 'aavegotchi',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "aavegotchi",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "aavegotchi"
