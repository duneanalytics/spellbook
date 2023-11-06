{% set blockchain = 'zksync' %}
{% set project_start_date_str = '2023-04-12' %}
{% set wrapper_token_address = '0x5aea5775959fbc2557cc8789bc1bf90a239d9a91' %}



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
