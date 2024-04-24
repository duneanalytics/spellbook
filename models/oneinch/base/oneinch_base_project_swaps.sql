{% set blockchain = 'base' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}


select 
    'sss' as blockchain
    , 'aaa' as tx_hash
    , 'bbb' as call_trace_address
    , array[(0x0000000000000000000000000000000000000000, true), (0xe4b5b3b8b5f6b7b1e4b5b3b8b5f6b7b1, false)] as call_transfer_addresses
    , date('2024-04-01') as block_month