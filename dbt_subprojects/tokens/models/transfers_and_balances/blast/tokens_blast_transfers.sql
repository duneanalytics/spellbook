{{config(
    schema = 'tokens_blast',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'table',
    file_format = 'delta',
    tags=['static'],
    post_hook='{{ hide_spells() }}'
    )
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_blast_base_transfers')
        , transfers_start_date = '2020-04-22'
        , blockchain = 'blast'
    )
}}
