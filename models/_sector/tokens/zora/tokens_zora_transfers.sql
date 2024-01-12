{{config(
    schema = 'tokens_zora',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='zora',
    transfers_base = ref('tokens_zora_base_transfers'),
    native_symbol = 'ETH'
)}}
