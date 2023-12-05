{{config(
    schema = 'tokens_fantom',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='fantom',
    transfers_base = ref('tokens_fantom_base_transfers'),
    native_symbol = 'FTM'
)}}
