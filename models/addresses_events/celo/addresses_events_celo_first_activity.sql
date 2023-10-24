{{
    config(
        tags = ['dunesql'],
        schema = 'addresses_events_celo',
        alias = alias('first_activity'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'append',
        unique_key = ['address']
    )
}}

{{
    addresses_events_first_activity(
        blockchain = 'celo',
        native_symbol = 'celo'
    )
}}
