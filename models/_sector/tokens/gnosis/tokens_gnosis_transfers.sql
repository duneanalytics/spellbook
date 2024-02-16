{{config(
    schema = 'tokens_gnosis',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='gnosis',
    transfers_base = ref('tokens_gnosis_base_transfers'),
    native_symbol = 'xDAI'
)}}
