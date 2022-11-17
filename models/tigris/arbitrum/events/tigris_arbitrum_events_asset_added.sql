{{ config(
    materialized = 'view',
    alias = 'asset_added',
    unique_key = ['evt_tx_hash', 'asset_id', 'pair']
    )
 }}

SELECT 
    evt_tx_hash, 
    _asset as asset_id, 
    _name as pair 
FROM 
{{ source('tigristrade_arbitrum', 'PairsContract_evt_AssetAdded') }}
