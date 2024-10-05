{% set blockchain = 'scroll' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        merge_update_columns = ['executed_tx_count', 'max_nonce', 'is_smart_contract', 'namespace', 'name', 'first_funded_by', 'first_funded_by_block_time', 'tokens_received_count', 'tokens_received_tx_count', 'tokens_sent_count', 'tokens_sent_tx_count', 'first_transfer_block_time', 'last_transfer_block_time', 'first_received_block_number', 'last_received_block_number', 'first_sent_block_number', 'last_sent_block_number', 'received_volume_usd', 'sent_volume_usd', 'first_tx_block_time', 'last_tx_block_time', 'first_tx_block_number', 'last_tx_block_number', 'last_seen', 'last_seen_block'],
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
