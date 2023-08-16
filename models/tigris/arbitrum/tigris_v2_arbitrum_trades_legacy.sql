{{ config(
	tags=['legacy'],
    schema = 'tigris_v2_arbitrum',
    alias = alias('trades', legacy_model=True)
    )
}}

SELECT 
    1 as dummy