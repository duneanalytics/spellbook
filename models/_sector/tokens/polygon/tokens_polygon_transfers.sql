{{config(
    schema = 'tokens_polygon',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='polygon',
    transfers_base = ref('tokens_polygon_base_transfers'),
    native_symbol = 'MATIC'
)}}
