{{config(
    schema = 'tokens_fantom',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'table',
    tags = ['static'],
    file_format = 'delta'
    , post_hook='{{ hide_spells() }}'
    )
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_fantom_base_transfers')
        , transfers_start_date = '2019-12-27'
        , blockchain = 'fantom'
    )
}}
