{{ config(
	tags=['legacy'],
    schema = 'tigris_polygon',
    alias = alias('events_limit_cancel', legacy_model=True)
    )
}}

SELECT 
    1 as dummy