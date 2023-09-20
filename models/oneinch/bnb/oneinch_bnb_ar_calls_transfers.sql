{% set blockchain = 'bnb' %}
{% set project_start_date_str = '2021-02-18' %}
{% set wrapper_token_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' %}



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
