{% set blockchain = 'story' %}

{{ config(
    schema = 'gas_' + blockchain
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'table'
    , tags = ['static']
    ,file_format = 'delta'
    )
}}

{{ evm_l1_gas_fees(blockchain) }}
