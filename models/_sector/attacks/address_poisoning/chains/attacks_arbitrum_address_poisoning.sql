{% set blockchain = 'arbitrum' %}

{{ config(
        
        schema = 'attacks_' + blockchain,
        alias = 'address_poisoning',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_index', 'evt_index']
)
}}

{{attacks_address_poisoning(
        blockchain = blockchain
        , transfers = ref('tokens_' + blockchain + '_transfers')
        , addresses = ref('cex_' + blockchain + '_addresses')
)}}