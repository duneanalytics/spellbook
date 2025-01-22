{{
    config(
	    schema = 'yield_yak',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c", "mantle"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1", "Henrystats", "hosuke"]\') }}'
        )
}}

SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'yield_yak'
