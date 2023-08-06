{{ config(
	tags=['legacy'],
    schema = 'tigris_arbitrum',
    alias = alias('events_limit_order', legacy_model=True)
    )
}}

SELECT 
    1 as dummy