{% set blockchain = 'arbitrum' %}
{% set project_start_date_str = '2021-09-14' %}
{% set wrapper_token_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' %}



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
