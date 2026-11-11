{% set blockchain = 'blast' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    
    , alias = 'first_token_received'
    , materialized = 'table'
    , file_format = 'delta'
    , partition_by = ['block_month']
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}


{{addresses_events_first_token_received(
    blockchain = blockchain
    , token_transfers = source('tokens_' + blockchain, 'transfers')
)}}