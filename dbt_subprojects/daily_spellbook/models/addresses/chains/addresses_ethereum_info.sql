{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        merge_update_columns = ['executed_tx_count', 'max_nonce', 'is_smart_contract', 'namespace', 'name', 'last_seen', 'last_tx_block_number'],
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
