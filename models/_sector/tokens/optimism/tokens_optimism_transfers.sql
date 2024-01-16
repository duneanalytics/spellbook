{{config(
    schema = 'tokens_optimism',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='optimism',
    transfers_base = ref('tokens_optimism_base_transfers'),
    native_symbol = 'ETH'
)}}
