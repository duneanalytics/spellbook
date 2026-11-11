{% set blockchain = 'nova' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    
    , alias = 'first_token_received'
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    , partition_by = ['block_month']
    )
}}


{{addresses_events_first_token_received(
    blockchain = blockchain
    , token_transfers = source('tokens_' + blockchain, 'transfers')
)}} 