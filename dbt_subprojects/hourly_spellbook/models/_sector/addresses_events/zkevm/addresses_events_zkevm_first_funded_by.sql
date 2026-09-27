{% set blockchain = 'zkevm' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    , alias = 'first_funded_by'
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}


{{addresses_events_first_funded_by(
    blockchain = blockchain
    , token_transfers = source('tokens_' + blockchain, 'transfers')
)}}
