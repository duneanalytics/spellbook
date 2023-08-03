{{ config(
	tags=['legacy'],
    schema = 'tigris_arbitrum',
    alias = alias('positions_liquidation', legacy_model=True)
    )
 }}

WITH 

SELECT 
    1 