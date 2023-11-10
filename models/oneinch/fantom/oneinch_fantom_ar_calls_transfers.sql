{% set blockchain = 'fantom' %}
{% set project_start_date_str = '2021-12-24' %}
{% set wrapper_token_address = '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' %}



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
