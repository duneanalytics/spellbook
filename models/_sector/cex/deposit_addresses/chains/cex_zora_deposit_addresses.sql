{% set blockchain = 'zora' %}

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
        , token_transfers = ref('tokens_' + blockchain + '_transfers')
        , cex_addresses = ref('cex_' + blockchain + '_addresses')
        , cex_flows = ref('cex_' + blockchain + '_flows')
        , first_funded_by = ref('addresses_events_' + blockchain + '_first_funded_by')
)}}