{% set blockchain = 'arbitrum' %}

{{ config(
        
        schema = 'attacks_' + blockchain,
        alias = 'address_poisoning',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index']
)
}}

{{attacks_address_poisoning(
        blockchain = blockchain
        , first_funded_by = source('addresses_events_' + blockchain, 'first_funded_by')
)}}