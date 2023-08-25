{{ config(
	tags=['legacy'],
    schema = 'tigris_polygon',
    alias = alias('positions_close', legacy_model=True)
    )
 }}

SELECT 
     1 as dummy