{{ config(
    schema = 'nft_polygon',
    
    alias = 'trades_beta_ported',
    materialized = 'view'
    )
}}

{{ port_to_old_schema(ref('nft_polygon_trades_beta')) }}
