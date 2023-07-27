{{config(
	tags=['legacy'],
	alias = alias('arbitrage_traders', legacy_model=True),
    post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["alexth", "hosuke"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_arbitrage_traders_ethereum_legacy') }}