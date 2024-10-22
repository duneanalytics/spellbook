{% set blockchain = 'bnb' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
    )
}}

{{
    addresses_info(
        blockchain = blockchain
        , transactions = source(blockchain, 'transactions')
        , token_transfers = source('tokens_' + blockchain, 'transfers')
        , creation_traces = source(blockchain, 'creation_traces')
        , first_funded_by = source('addresses_events_' + blockchain, 'first_funded_by')
        , contracts = source(blockchain, 'contracts')
    )
}}
