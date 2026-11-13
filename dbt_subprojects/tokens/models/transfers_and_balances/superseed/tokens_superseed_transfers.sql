{{config(
    schema = 'tokens_superseed',
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
        base_transfers = ref('tokens_superseed_base_transfers')
        , transfers_start_date = '2025-05-08'
        , blockchain = 'superseed'
    )
}}