{% set blockchain = 'arbitrum' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'test_explicit',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'call_trace_address', 'transfer_trace_address']
    )
}}



{{ 
    oneinch_call_transfers_explicit_macro(
        blockchain = blockchain
        , first_deploy_at = "timestamp '2021-06-22 10:27'"
        , wrapped_native_token_address='0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
    )
}}