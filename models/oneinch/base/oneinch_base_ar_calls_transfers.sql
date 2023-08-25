{% set blockchain = 'base' %}
{% set project_start_date_str = '2023-08-08' %}
{% set wrapper_token_address = '0x4200000000000000000000000000000000000006' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = alias('ar_calls_transfers'),
        tags = ['dunesql'],
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['unique_call_transfer_id']
    )
}}



{{ 
    oneinch_ar_calls_transfers_macro(
        blockchain = blockchain,
        project_start_date_str = project_start_date_str,
        wrapper_token_address = wrapper_token_address
    )
}}
