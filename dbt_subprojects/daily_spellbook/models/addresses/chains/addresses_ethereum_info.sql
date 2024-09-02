{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'delete+insert',
        unique_key = ['address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.last_seen')]
    )
}}

{{
    addresses_info(
        blockchain = blockchain
        , transactions = source(blockchain, 'transactions')
        , token_transfers = source('tokens_' + blockchain, 'transfers')
        , creation_traces = source(blockchain, 'creation_traces')
        , first_funded_by = ref('addresses_events_' + blockchain + '_first_funded_by')
        , contracts = source(blockchain, 'contracts')
    )
}}
