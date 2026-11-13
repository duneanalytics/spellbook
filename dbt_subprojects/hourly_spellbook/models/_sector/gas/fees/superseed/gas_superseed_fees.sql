{% set blockchain = 'superseed' %}

{{ config(
    schema = 'gas_' + blockchain
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'table'
    , tags = ['static']
    ,file_format = 'delta'
    )
}}
{{ op_stack_gas_fees(blockchain) }}
