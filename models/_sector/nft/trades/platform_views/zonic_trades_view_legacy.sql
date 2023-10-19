
{{ config(
	tags=['legacy'],
	
        schema = 'zonic',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "zonic",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades_legacy') }}
WHERE project = "zonic"
