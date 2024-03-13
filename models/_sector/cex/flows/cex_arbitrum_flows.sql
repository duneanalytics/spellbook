{% set blockchain = 'arbitrum' %}

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
        , transfers = ref('tokens_arbitrum_transfers')
        , addresses = ref('cex_arbitrum_addresses')
)}}