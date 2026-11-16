{% set blockchain = 'nova' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    , alias = 'first_funded_by'
    , materialized = 'table'
    , tags = ['static']
    , file_format = 'delta'
    )
}}


{{addresses_events_first_funded_by(
    blockchain = blockchain
    , token_transfers = source('tokens_' + blockchain, 'transfers')
)}}
