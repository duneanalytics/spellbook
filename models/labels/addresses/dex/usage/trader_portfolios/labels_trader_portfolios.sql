{{
    config(
	tags=['legacy'],
	
        alias = alias('trader_portfolios', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_trader_portfolios_ethereum_legacy') }}