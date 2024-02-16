{{config(
    schema = 'tokens_arbitrum',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='arbitrum',
    transfers_base = ref('tokens_arbitrum_base_transfers'),
    native_symbol = 'ETH'
)}}
