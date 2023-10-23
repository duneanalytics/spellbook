{% set blockchain = 'polygon' %}
{% set project_start_date_str = '2021-04-14' %}
{% set wrapper_token_address = '0x0000000000000000000000000000000000001010' %}



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
