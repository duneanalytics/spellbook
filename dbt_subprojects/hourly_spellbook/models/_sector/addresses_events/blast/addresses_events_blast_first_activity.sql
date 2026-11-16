{{ config(
    schema = 'addresses_events_blast'
    , alias = 'first_activity'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{addresses_events_first_activity(
    blockchain='blast',
    native_symbol = 'eth'
)}}
