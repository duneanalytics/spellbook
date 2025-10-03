{{config(
    schema = 'tokens_blast',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
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
