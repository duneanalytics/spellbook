{% set blockchain = 'zora' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    
    , alias = 'first_token_received'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , partition_by = ['block_month']
    , unique_key = ['address']
    )
}}


{{addresses_events_first_token_received(
    blockchain = blockchain
    , token_transfers = source('tokens_' + blockchain, 'transfers')
)}} 