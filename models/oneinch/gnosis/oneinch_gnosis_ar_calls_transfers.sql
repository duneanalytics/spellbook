{% set blockchain = 'gnosis' %}
{% set project_start_date_str = '2022-01-14' %}
{% set wrapper_token_address = '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'ar_calls_transfers',
        
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
