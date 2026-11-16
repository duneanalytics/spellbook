{{config(
    schema = 'tokens_nova'
    , alias = 'transfers'
    , partition_by = ['block_month']
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_nova_base_transfers')
        , transfers_start_date = '2024-12-06'
        , blockchain = 'nova'
    )
}} 