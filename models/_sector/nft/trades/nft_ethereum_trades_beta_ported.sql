{{ config(
    schema = 'nft_ethereum',
    tags = ['dunesql'],
    alias = alias('trades_beta_ported'),
    materialized = 'view'
    )
}}

{{ port_to_old_schema(ref('nft_ethereum_trades_beta')) }}
