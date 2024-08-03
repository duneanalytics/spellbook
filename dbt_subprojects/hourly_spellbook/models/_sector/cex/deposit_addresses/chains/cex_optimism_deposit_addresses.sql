{% set blockchain = 'optimism' %}

{{ config(
        
        schema = 'cex_' + blockchain,
        alias = 'deposit_addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
)
}}

{{cex_deposit_addresses(
        blockchain = blockchain
        , transactions = source(blockchain, 'transactions')
        , token_transfers = source('tokens_' + blockchain, 'transfers')
        , cex_addresses = ref('cex_' + blockchain + '_addresses')
        , cex_flows = ref('cex_' + blockchain + '_flows')
        , first_funded_by = source('addresses_events_' + blockchain, 'first_funded_by')
        , creation_traces = source(blockchain, 'creation_traces')
)}}