{% set blockchain = 'nova' %}

{{config(
        schema = 'cex_' + blockchain,
        alias = 'addresses',
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
        )}}

{{cex_evms(
        cex_addresses = ref('cex_evms_addresses')
        , blockchain = blockchain
        , token_transfers = source('tokens_'~blockchain,'transfers')
        , contract_creations =  source(blockchain, 'creation_traces')
        )}}