{{ config(
    schema = 'addresses_events_berachain'
    
    , alias = 'first_activity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

{{addresses_events_first_activity(
    blockchain='berachain',
    native_symbol = 'BERA'
)}}
