{% set blockchain = 'arbitrum' %}
{% set project_start_date_str = '2021-09-14' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = alias('calls_transfers'),
        tags = ['dunesql'],
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['unique_call_transfer_id']
    )
}}



{{ 
    oneinch_calls_transfers_macro(
        blockchain = blockchain,
        project_start_date_str = project_start_date_str
    )
}}
