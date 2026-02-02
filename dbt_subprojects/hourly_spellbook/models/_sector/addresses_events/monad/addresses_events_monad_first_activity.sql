{% set blockchain = 'monad' %}

{{ config(
    schema = 'addresses_events_' + blockchain
    , alias = 'first_activity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

{{addresses_events_first_activity(
    blockchain = blockchain
    , native_symbol = 'MON'
)}}
