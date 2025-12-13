{% set blockchain = 'megaeth' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces_base_wrapper_deposits',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_date', 'unique_key'],
    )
}}

{{ transfers_from_traces_base_wrapper_deposits_macro(
    blockchain = blockchain, 
    transfers_from_traces_base_table = ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base')
) }}
