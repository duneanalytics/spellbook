{{config(
    schema = 'tokens_avalanche_c',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='avalanche_c',
    transfers_base = ref('tokens_avalanche_c_base_transfers'),
    native_symbol = 'AVAX'
)}}
