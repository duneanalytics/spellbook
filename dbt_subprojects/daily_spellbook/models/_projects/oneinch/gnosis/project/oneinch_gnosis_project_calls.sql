{% set blockchain = 'gnosis' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_calls',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'call_trace_address']
    )
}}

-- depends_on: {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}


{{
    oneinch_project_calls_macro(
        blockchain = blockchain
    )
}}