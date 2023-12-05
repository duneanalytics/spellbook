{{config(
    schema = 'tokens_base',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='base',
    transfers_base = ref('tokens_base_base_transfers'),
    native_symbol = 'ETH'
)}}
