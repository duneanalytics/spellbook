{% set blockchain = 'bnb' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'test_single_table',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'call_trace_address', 'transfer_trace_address']
    )
}}



{{ 
    oneinch_call_transfers_macro(
        blockchain = blockchain
        , blockchain_meta = ref('oneinch_test_dict_table')
    )
}}