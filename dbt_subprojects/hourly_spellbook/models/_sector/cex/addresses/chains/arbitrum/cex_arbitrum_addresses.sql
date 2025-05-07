{% set blockchain = 'arbitrum' %}

{{config(
        schema = 'cex_' + blockchain,
        alias = 'addresses',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'address']
        )}}

{{cex_evms(
        cex_addresses = ref('cex_evms_addresses')
        , blockchain = blockchain
        , traces = source(blockchain, 'traces')
        )}}