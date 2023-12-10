{% set blockchain = 'ethereum' %}
{% set project_start_date_str = '2019-06-03' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'calls_transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address', 'transfer_trace_address']
    )
}}



{{ 
    oneinch_calls_transfers_macro(
        blockchain = blockchain,
        project_start_date_str = project_start_date_str
    )
}}
