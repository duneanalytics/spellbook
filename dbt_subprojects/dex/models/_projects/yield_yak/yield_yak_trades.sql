{{
    config(
	    schema = 'yield_yak',
        alias = 'trades',
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
        )
}}

SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'yield_yak'
