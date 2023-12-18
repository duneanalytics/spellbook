{{config(
    schema = 'tokens_bnb',
    alias = 'transfers',
    partition_by = ['token_standard', 'block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['unique_key'],
)
}}

{{transfers_enrich(
    blockchain='bnb',
    transfers_base = ref('tokens_bnb_base_transfers'),
    native_symbol = 'BNB'
)}}