
{{ config(
	tags=['legacy'],
	
        schema = 'stealcam',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "stealcam",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "stealcam"
