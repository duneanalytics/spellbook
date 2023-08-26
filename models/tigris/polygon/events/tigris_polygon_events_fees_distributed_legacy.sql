{{ config(
	tags=['legacy'],
    schema = 'tigris_polygon',
    alias = alias('events_fees_distributed', legacy_model=True)
    )
}}

SELECT 
    1  as dummy