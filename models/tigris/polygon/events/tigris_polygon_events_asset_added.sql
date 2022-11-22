{{ config(
    materialized = 'view',
    alias = 'polygon_events_asset_added',
    unique_key = ['evt_tx_hash', 'asset_id', 'pair']
    )
 }}

SELECT 
    evt_tx_hash, 
    _asset as asset_id, 
    _name as pair 
FROM 
{{ source('tigristrade_polygon', 'PairsContract_evt_AssetAdded') }}
