{{ config(
    schema = 'nft',
    alias ='trades_events_forward',
    materialized = 'view'
    )
}}

{{ port_to_new_schema(ref('nft_events_old')) }}
