{{ config(
    tags=['dunesql'],
    schema = 'tigris_polygon',
    alias = alias('events_asset_added')
    )
 }}

SELECT 
    '1' as protocol_version, 
    _asset as asset_id, 
    _name as pair
FROM 
{{ source('tigristrade_polygon', 'PairsContract_evt_AssetAdded') }}

UNION

SELECT 
    '1' as protocol_version,
    UINT256 '33' as asset_id,
    'LINK/BTC' as pair

UNION 

SELECT 
    '1' as protocol_version, 
    UINT256 '34' as asset_id,
    'XMR/BTC' as pair 

UNION 

SELECT 
    '2' as protocol_version, 
    _asset as asset_id, 
    _name as pair
FROM 
{{ source('tigristrade_v2_polygon', 'PairsContract_evt_AssetAdded') }}

UNION 

SELECT 
    '2' as protocol_version, 
    _asset as asset_id, 
    _name as pair
FROM 
{{ source('tigristrade_v2_polygon', 'PairsContract_v2_evt_AssetAdded') }}
