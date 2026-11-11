{% set blockchain = 'nova' %}

{{ config(
        
        schema = 'cex_' + blockchain,
        alias = 'deposit_addresses',
        tags = ['prod_exclude', 'static'],
        materialized = 'table',
        file_format = 'delta',
)
}}

{{cex_deposit_addresses(
        blockchain = blockchain
        , cex_local_flows = source('cex_' + blockchain, 'flows')
)}} 