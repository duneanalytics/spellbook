{{ config(
	tags=['legacy'],
    schema = 'tigris_polygon',
    alias = alias('positions_liquidation', legacy_model=True)
    )
 }}

WITH 

SELECT 
    1 