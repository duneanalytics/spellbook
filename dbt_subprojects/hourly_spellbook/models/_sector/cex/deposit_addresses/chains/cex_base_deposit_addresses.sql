{% set blockchain = 'base' %}

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
        , cex_local_flows = ref('cex_' + blockchain + '_flows')
        , crosschain_first_funded_by = ref('addresses_events_first_funded_by')
)}}