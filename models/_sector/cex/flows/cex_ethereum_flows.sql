{% set blockchain = 'ethereum' %}

{{ config(
        
        schema = 'cex_' + blockchain,
        alias = 'flows',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['flow_type', 'unique_key']
)
}}

{{cex_flows(
        blockchain = blockchain
        , transfers = ref('tokens_ethereum_transfers')
        , addresses = ref('cex_ethereum_addresses')
)}}