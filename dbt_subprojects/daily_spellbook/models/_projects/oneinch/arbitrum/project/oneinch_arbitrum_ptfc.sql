{% set blockchain = 'arbitrum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'ptfc',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'block_number', 'transfer_from', 'tx_hash', 'transfer_trace_address']
    )
}}

-- depends_on: {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}

select *, date(date_trunc('month', block_time)) as block_month from (
{{
    oneinch_project_ptfc_macro(
        blockchain = blockchain
    )
}}
)
where block_time >= date('2025-08-15')