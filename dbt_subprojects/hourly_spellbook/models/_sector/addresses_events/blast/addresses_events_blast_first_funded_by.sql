{% set blockchain = 'blast' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    , alias = 'first_funded_by'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}


{{addresses_events_first_funded_by(
    blockchain = blockchain
    , token_transfers = source('tokens_' + blockchain, 'transfers')
)}} 