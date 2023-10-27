{% set blockchain = 'avalanche_c' %}
{% set project_start_date_str = '2021-11-20' %}
{% set wrapper_token_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' %}



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
