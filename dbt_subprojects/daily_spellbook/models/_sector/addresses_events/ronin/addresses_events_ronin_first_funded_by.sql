{% set blockchain = 'ronin' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    , alias = 'first_funded_by'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}


{{addresses_events_first_funded_by(
    blockchain = blockchain
    , token_transfers = source('tokens_' + blockchain, 'transfers')
)}}
