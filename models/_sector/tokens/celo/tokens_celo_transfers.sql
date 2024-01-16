{{config(
    schema = 'tokens_celo',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='celo',
    transfers_base = ref('tokens_celo_base_transfers'),
    native_symbol = 'CELO'
)}}
