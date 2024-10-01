{% set blockchain = 'celo' %}

{{
    config(
        tags=['prod_exclude'],
        schema = 'addresses_' + blockchain,
        alias = 'info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        merge_update_columns = ['executed_tx_count', 'max_nonce', 'is_smart_contract', 'namespace', 'name', 'first_funded_by', 'received_count', 'sent_count', 'first_received_block_time', 'last_received_block_time', 'first_sent_block_time', 'last_sent_block_time', 'received_volume_usd', 'sent_volume_usd', 'first_tx_block_time', 'last_tx_block_time', 'first_tx_block_number', 'last_tx_block_number', 'last_seen'],
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
