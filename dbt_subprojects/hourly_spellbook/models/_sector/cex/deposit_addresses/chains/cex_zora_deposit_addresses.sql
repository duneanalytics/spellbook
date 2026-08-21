{% set blockchain = 'zora' %}

{{ config(
        
        schema = 'cex_' + blockchain,
        alias = 'deposit_addresses',
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
)
}}

{{cex_deposit_addresses(
        blockchain = blockchain
        , cex_local_flows = ref('cex_' + blockchain + '_flows')
)}}