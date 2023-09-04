{{ config(
	tags=['legacy'],
    schema = 'tigris_arbitrum',
    alias = alias('events_asset_added', legacy_model=True)
    )
 }}

SELECT 
    1 as dummy