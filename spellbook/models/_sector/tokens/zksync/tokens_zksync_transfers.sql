{{config(
    schema = 'tokens_zksync',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{transfers_enrich(
    blockchain='zksync',
    transfers_base = ref('tokens_zksync_base_transfers'),
    native_symbol = 'ETH'
)}}
