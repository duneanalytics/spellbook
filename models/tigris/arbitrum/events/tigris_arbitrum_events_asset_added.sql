{{ config(
    tags=['dunesql'],
    schema = 'tigris_arbitrum',
    alias = alias('events_asset_added')
    )
 }}

SELECT 
    '1' as protocol_version, 
    _asset as asset_id, 
    _name as pair
FROM 
{{ source('tigristrade_arbitrum', 'PairsContract_evt_AssetAdded') }}

UNION

SELECT 
    '2' as protocol_version, 
    _asset as asset_id, 
    _name as pair
FROM 
{{ source('tigristrade_v2_arbitrum', 'PairsContract_evt_AssetAdded') }}

UNION 

SELECT 
    '2' as protocol_version, 
    _asset as asset_id, 
    _name as pair
FROM 
{{ source('tigristrade_v2_arbitrum', 'PairsContract_v2_evt_AssetAdded') }}
