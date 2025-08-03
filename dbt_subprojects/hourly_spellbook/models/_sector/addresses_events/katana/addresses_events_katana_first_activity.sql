{{ config(
    schema = 'addresses_events_katana'
    
    , alias = 'first_activity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

{{addresses_events_first_activity(
    blockchain='katana',
    native_symbol = 'ETH'
)}}
