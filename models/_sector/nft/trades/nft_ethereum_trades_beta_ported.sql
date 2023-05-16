{{ config(
    schema = 'nft_ethereum',
    alias ='trades_beta_ported',
    materialized = 'view'
    )
}}

select * from
{{ port_to_old_schema(ref('nft_ethereum_tradeS_beta')) }}
