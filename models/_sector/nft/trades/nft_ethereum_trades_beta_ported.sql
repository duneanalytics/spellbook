{{ config(
    schema = 'nft_ethereum',
    alias ='trades_beta_ported',
    materialized = 'view'
    )
}}

with nft_events_forward_ported as
({{ port_to_old_schema(ref('nft_events')) }})

select


from nft_events_forward_ported
