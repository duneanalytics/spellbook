{% set blockchain = 'zora' %}

{{ config(
        
        schema = 'cex_' + blockchain,
        alias = 'deposit_addresses',
        materialized = 'table',
        tags = ['static', 'prod_exclude'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
)
}}

{{cex_deposit_addresses(
        blockchain = blockchain
        , cex_local_flows = source('cex_' + blockchain, 'flows')
)}}