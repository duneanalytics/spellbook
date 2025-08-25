{{config(
    tags = ['static'],
    schema = 'tokens_goerli',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_goerli_base_transfers')
        , transfers_start_date = '2019-01-30'
        , blockchain = 'goerli'
    )
}}