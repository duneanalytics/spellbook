{% set blockchain = 'arbitrum' %}



{{ 
    config( 
        schema = 'oneinch',
        alias = 'mart_view_from_table',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'transfer_trace_address']
    )
}}


{{ 
    oneinch_test_macro(
        blockchain = blockchain
        , const = ref('oneinch_dict_view_from_table')
    )
}}