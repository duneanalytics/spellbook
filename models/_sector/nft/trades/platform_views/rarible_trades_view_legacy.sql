
{{ config(
	tags=['legacy'],
	
        schema = 'rarible',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "rarible",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "rarible"
