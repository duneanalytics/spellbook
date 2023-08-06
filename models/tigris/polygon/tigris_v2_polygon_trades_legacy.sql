{{ config(
	tags=['legacy'],
    schema = 'tigris_v2_polygon',
    alias = alias('trades', legacy_model=True)
    )
}}

SELECT 
    1 as dummy