{{ config(
    
    schema = 'mux_protocol_optimism',
    alias = 'asset_added'
    )
 }}

SELECT 
    id,
    symbol
FROM 
{{ source('mux_optimism', 'LiquidityPoolHop1_evt_AddAsset') }}
