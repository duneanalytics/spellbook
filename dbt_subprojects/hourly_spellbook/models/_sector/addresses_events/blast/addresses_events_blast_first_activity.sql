{{ config(
    schema = 'addresses_events_blast'
    , alias = 'first_activity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{addresses_events_first_activity(
    blockchain='blast',
    native_symbol = 'eth'
)}}
