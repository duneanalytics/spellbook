{{config(
    schema = 'tokens_bnb',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='bnb',
    transfers_base = ref('tokens_bnb_base_transfers'),
    native_symbol = 'BNB'
)}}
