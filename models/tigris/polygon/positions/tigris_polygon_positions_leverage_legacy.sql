{{ config(
	tags=['legacy'],
    schema = 'tigris_polygon',
    alias = alias('positions_leverage', legacy_model=True)
    )
 }}

SELECT 
    1 as dummy