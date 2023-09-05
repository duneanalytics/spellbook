{{ config(
	tags=['legacy'],
    schema = 'tigris_polygon',
    alias = alias('options_limit_order', legacy_model=True)
    )
}}

SELECT 
    1 as dummy