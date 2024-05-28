{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    , alias = 'first_received'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

{{addresses_events_first_received(
    blockchain = blockchain
    , token_transfers = ref('tokens_' + blockchain +'_transfers')
)}}