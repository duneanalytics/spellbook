{% set blockchain = 'arbitrum' %}

{{ config(

        schema = 'cex_' + blockchain,
        alias = 'flows',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['flow_type', 'unique_key']
)
}}

{{cex_flows(
        blockchain = blockchain
        , transfers = source('tokens_' + blockchain , 'transfers')
        , addresses = ref('cex_' + blockchain + '_addresses')
)}}
