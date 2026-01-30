{{config(
    schema = 'tokens_zora',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , post_hook='{{ hide_spells() }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_zora_base_transfers')
        , transfers_start_date = '2023-06-13'
        , blockchain = 'zora'
    )
}}
