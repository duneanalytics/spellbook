
{{ config(
	tags=['legacy'],
	
        schema = 'x2y2',
        alias = alias('trades', legacy_model=True),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "x2y2",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "x2y2"
