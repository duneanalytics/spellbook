{% set blockchain = 'bnb' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'parsed_transfers_from_calls',
        partition_by = ['block_number'],
        materialized = 'view'
    )
}}



{{
    oneinch_parsed_transfers_from_calls_macro(
        blockchain = blockchain
    )
}}