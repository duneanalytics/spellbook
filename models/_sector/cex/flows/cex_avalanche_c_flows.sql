{% set blockchain = 'avalanche_c' %}

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
        , transfers = ref('tokens_avalanche_c_transfers')
        , addresses = ref('cex_avalanche_c_addresses')
)}}