{% set blockchain = 'linea' %}

{{config(
        schema = 'cex_' + blockchain,
        alias = 'addresses'
        )}}

{{cex_evms(
        cex_addresses = ref('cex_evms_addresses')
        , blockchain = blockchain
        , traces = source(blockchain, 'traces')
        )}}