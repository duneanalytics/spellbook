{{config(
    schema = 'tokens_ethereum',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='ethereum',
    transfers_base = ref('tokens_ethereum_base_transfers'),
    native_symbol = 'ETH',
)}}
