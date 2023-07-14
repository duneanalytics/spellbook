{{config(
	tags=['legacy'],
	alias = alias('smart_dex_traders', legacy_model=True), post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["stone"]\') }}')}}

SELECT * FROM {{ ref('labels_smart_dex_traders_ethereum_legacy') }}